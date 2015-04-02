#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "factor_coder.hpp"
#include "bit_streams.hpp"

#include <sdsl/suffix_arrays.hpp>
#include <sdsl/int_vector_mapper.hpp>

#include <cctype>



template <
uint32_t t_block_size = 1024,
class t_suffix_array = sdsl::csa_bitcompressed<>,
class t_factor_selector = factor_select_first
>
struct factor_strategy_greedy_backward_search {
	static std::string type() {
		return "factor_strategy_greedy_backward_search-"+std::to_string(t_block_size)+"-"+t_factor_selector::type();
	}

	template<class t_ostream,class t_coder, class t_itr>
	static uint32_t encode_block(t_ostream& ofs,const t_suffix_array& sa,
		t_itr itr,t_itr end,std::vector<factor_data>& factors) 
	{
		size_t num_factors = 0;
		size_t sp=0;
		size_t ep=sa.size()-1;
		auto factor_start = itr;
		while(itr != end) {
    		auto sym = *itr;
    		auto mapped_sym = sa.char2comp[sym];
			bool sym_exists_in_dict = mapped_sym != 0;
			size_t res_sp,res_ep;
			if(sym_exists_in_dict) {
				if(sp == 0 && ep == sa.size()-1) {
					// small optimization
					res_sp = sa.C[mapped_sym];
					res_ep = sa.C[mapped_sym+1]-1;
				} else {
					sdsl::backward_search(sa,sp,ep,sym,res_sp,res_ep);
				}
			}
			if( !sym_exists_in_dict || res_ep < res_sp ) { // not found
				auto factor_len = std::distance(factor_start,itr);
				if(factor_len == 0) { // unknown symbol
					factors[num_factors].offset = (uint32_t) sym;
					factors[num_factors].len = 0;
					++itr;
				} else {
					// substring not found
					factors[num_factors].offset = factor_select_first::pick_offset<>(sa,sp,ep,(size_t)factor_len,true);
					factors[num_factors].len = factor_len;
					sp = 0; ep = sa.size()-1;
					// don't increment itr as we still have to 
					// encode current symbol
				}
				num_factors++;
				factor_start = itr;
			} else { // found substring
				sp = res_sp; ep = res_ep;
				++itr;
			}
		}
		/* are we in a substring? encode the rest */
		if(factor_start != itr) {
			auto factor_len = std::distance(factor_start,itr);
			factors[num_factors].offset = factor_select_first::pick_offset<>(sa,sp,ep,(size_t)factor_len,true);
			factors[num_factors].len = (uint32_t) factor_len;
			num_factors++;
		}

		/* write the factors to the output stream */
		t_coder::template encode_block<t_ostream>(ofs,factors,num_factors);
		return num_factors;
	}

	template<typename t_coder>
	static void encode(collection& col) {
		using clock = std::chrono::high_resolution_clock;
		// (0) check if factorized text already exists.
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        auto file_name = col.path + "/index/"+type()+"-"+t_coder::type()+"-dhash="+dict_hash+".sdsl";
        col.file_map[KEY_FACTORIZED_TEXT] = file_name;
        col.file_map[KEY_BLOCKOFFSETS] = col.path+"/index/"+KEY_BLOCKOFFSETS+"-"+dict_hash+".sdsl";
        col.file_map[KEY_BLOCKFACTORS] = col.path+"/index/"+KEY_BLOCKFACTORS+"-"+dict_hash+".sdsl";
        if(!col.rebuild && utils::file_exists(file_name)) {
        	LOG(INFO) << "\tText already factorized in " << file_name;
        	return;
        }

		/* (1) create or load search structure */
		using sa_type = t_suffix_array;
		bool build_over_reverse_dict = true;
		sa_type sa = create_or_load_sa<sa_type>(col,build_over_reverse_dict);

        /* (2) perform factorization */
        const sdsl::int_vector_mapper<8,std::ios_base::in> text(col.file_map[KEY_TEXT]);
        auto F = sdsl::write_out_buffer<1>::create(col.file_map[KEY_FACTORIZED_TEXT]);
        using bit_ofs = bit_ostream<decltype(F)>; // stream output directly to disk
        bit_ofs ofs(F);

        /* (2a) iterate over all blocks and perform factorization */
        LOG(INFO) << "\tFactorize text in blocks of size " << t_block_size << " bytes";
        auto block_size = t_block_size;
        std::vector<factor_data> block_factors(block_size);
        auto num_blocks = text.size() / block_size;
        auto left = text.size() % block_size;
        sdsl::int_vector<> block_offsets(num_blocks + (left != 0));
        sdsl::int_vector<> factors_in_block(num_blocks + (left != 0));
        auto block_itr = text.begin();
        auto block_end = block_itr + block_size;
        
        auto time_last = clock::now();
        auto blocks_per_10mib = (10*1024*1024) / t_block_size;
        for(size_t i=0;i<num_blocks;i++) {

        	block_offsets[i] = ofs.tellp();
        	factors_in_block[i] = encode_block<bit_ofs,t_coder>(ofs,sa,block_itr,block_end,block_factors);
        	block_itr = block_end;
        	block_end += block_size;


            if( (i+1) % blocks_per_10mib == 0) { // output stats
                auto time_for_one_mib = clock::now() - time_last;
                time_last = clock::now();
                double time_in_sec = std::chrono::duration_cast<std::chrono::milliseconds>(time_for_one_mib).count() / 1000.0f;
                double percent_complete = 100.0*(i+1)/(double)num_blocks;
                LOG(INFO) << "\t" << "Current factorization speed = " 
                          << std::setprecision(4) << 10.0/time_in_sec << " MiB/s ("
                          << std::setprecision(3) << percent_complete << "% done)";
            }
        }
        if(left != 0) { /* is there a non-full block? */
        	block_offsets[num_blocks] = ofs.tellp();
			factors_in_block[num_blocks] = encode_block<bit_ofs,t_coder>(ofs,sa,block_itr,block_itr+left+1,block_factors);
        }

        // store block offsets
        LOG(INFO) << "\tStore block offsets";
        sdsl::util::bit_compress(block_offsets);
        sdsl::util::bit_compress(factors_in_block);
        sdsl::store_to_file(block_offsets,col.file_map[KEY_BLOCKOFFSETS]);
        sdsl::store_to_file(factors_in_block,col.file_map[KEY_BLOCKFACTORS]);
	}
};
