#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "factor_coder.hpp"
#include "bit_streams.hpp"

#include <sdsl/suffix_arrays.hpp>
#include <sdsl/int_vector_mapper.hpp>

#include <cctype>

using clock = std::chrono::high_resolution_clock;

struct encoded_factors {
	uint64_t offset;
	uint64_t block_size;
	uint64_t total_encoded_factors = 0;
	uint64_t total_encoded_blocks = 0;
	uint64_t cur_factors_in_block = 0;
	std::chrono::nanoseconds encoding_start;
	std::vector<factor_data> tmp_factor_buffer;
    sdsl::int_vector_mapper<1> factored_text;
    sdsl::int_vector_mapper<64> block_offsets;
    sdsl::int_vector_mapper<64> block_factors;
    bit_ostream<sdsl::int_vector_mapper<1>> factor_stream;
	encoded_factors(collection& col,size_t _block_size,size_t _offset) 
		: block_size(_block_size), offset(_offset),
		  factored_text(sdsl::write_out_buffer<1>::create(col.temp_file_name(KEY_FACTORIZED_TEXT,offset))),
		  block_offsets(sdsl::write_out_buffer<64>::create(col.temp_file_name(KEY_BLOCKOFFSETS,offset))),
		  block_factors(sdsl::write_out_buffer<64>::create(col.temp_file_name(KEY_BLOCKFACTORS,offset))),
		  factor_stream(factored_text_block)
	{
		// create a buffer we can write to without reallocating
		tmp_factor_buffer.resize(block_size);
		// save the start of the encoding process
		encoding_start = clock::now();
	}
	void add_to_block_factor(uint32_t offset,uint32_t len) {
		tmp_factor_buffer[cur_factors_in_block].offset = offset;
		tmp_factor_buffer[cur_factors_in_block].len = len;
		cur_factors_in_block++;
	}
	void start_new_block() {
		cur_factors_in_block = 0;
	}
	template<class t_coder>
	void encode_current_block() {
		block_offsets[total_encoded_blocks] = factor_stream.tellp();
		block_factors[total_encoded_blocks] = cur_factors_in_block;

		total_encoded_factors += cur_factors_in_block;
		total_encoded_blocks++;
		t_coder::template encode_block<bit_ostream<sdsl::int_vector_mapper<1>>>(factor_stream,
			tmp_factor_buffer,cur_factors_in_block);
	}
	void output_stats() const {
		auto cur_time = clock::now();
		auto time_spent = cur_time - encoding_start;
		auto time_in_sec = std::chrono::duration_cast<std::chrono::milliseconds>(time_spent).count() / 1000.0f;
		auto bytes_written = factor_stream.tellp() / 8;
		auto mb_written = bytes_written / (1024*1024);
		LOG(INFO) << "(" << offset << ") "
				  << "FACTORS = " << total_encoded_factors << " "
				  << "BLOCKS = " << total_encoded_blocks << " "
				  << "F/B = " << (double)total_encoded_factors/(double)total_encoded_blocks << " "
				  << "SPEED = " << mb_written / time_in_sec << " MB/s";
	}
};

template <
uint32_t t_block_size = 1024,
class t_index = dict_index_csa,
class t_factor_selector = factor_select_first
>
struct factorizor {
	static std::string type() {
		return "factor_strategy_greedy_backward_search-"+std::to_string(t_block_size)+"-"+t_factor_selector::type();
	}

	static uint32_t encode_block(encoded_factors& ef,const t_index& idx,t_itr itr,t_itr end) 
	{
		auto factor_itr = idx.factorize(itr,end);
		ef.start_new_block();
		while(factor_itr.valid()) {
			if( factor_itr.len == 0) {
				ef.add_to_block_factor(factor_itr.sym,0);
			} else {
				auto offset = factor_select_first::pick_offset<>(sa,factor_itr.sp,factor_itr.ep,factor_itr.len);
				ef.add_to_block_factor(offset,factor_itr.len);
			}

			factor_itr++;
		}
		ef.encode_current_block<t_coder>();
	}

	template<typename t_coder,class t_itr>
	static encoded_factors 
	encode(collection& col,t_index& idx,t_itr itr,t_itr end,size_t offset = 0) {
		/* (1) create output files */
		encoded_factors ef(col,offset);

		/* (2) compute text stats */
		auto block_size = t_block_size;
		auto n = std::distance(itr,end);
		auto num_blocks = n / block_size;
		auto left = n % block_size;
		auto blocks_per_10mib = (10*1024*1024) / block_size;

		/* (3) encode blocks */
		for(size_t i=1;i<=num_blocks;i++) {
	        auto block_end = itr + block_size;
			encode_block<t_coder>(ef,idx,itr,block_end);
        	itr = block_end;
        	block_end += block_size;

        	if(i % blocks_per_10mib == 0) {
        		ef.output_stats();
        	}
		}

		/* (4) is there a non-full block? */
		if(left != 0) { 
			encode_block<t_coder>(ef,idx,itr,itr+left+1);
		}
		return ef;
	}
};
