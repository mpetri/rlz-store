#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "factor_coder.hpp"
#include "bit_streams.hpp"
#include "factor_storage.hpp"

#include <sdsl/suffix_arrays.hpp>
#include <sdsl/int_vector_mapper.hpp>

#include <cctype>

template <
uint32_t t_block_size = 1024,
class t_index = dict_index_csa<>,
class t_factor_selector = factor_select_first
>
struct factorizor {
	static std::string type() {
		return "factorizor-"+std::to_string(t_block_size)+"-"+t_factor_selector::type();
	}

	template<class t_coder,class t_itr>
	static void factorize_block(factor_storage& fs,const t_index& idx,t_itr itr,t_itr end) 
	{
		auto factor_itr = idx.factorize(itr,end);
		fs.start_new_block();
		size_t i=0;
		while(! factor_itr.finished() ) {
			if( factor_itr.len == 0) {
				fs.add_to_block_factor(factor_itr.sym,0);
			} else {
				auto offset = t_factor_selector::template pick_offset<>(idx,factor_itr.sp,factor_itr.ep,factor_itr.len);
				fs.add_to_block_factor(offset,factor_itr.len);
			}
			++factor_itr;
			i++;
		}
		fs.encode_current_block<t_coder>();
	}

	template<typename t_coder,class t_itr>
	static factorization_info 
	factorize(collection& col,t_index& idx,t_itr itr,t_itr end,size_t offset = 0) {
		/* (1) create output files */
		factor_storage fs(col,t_block_size,offset);

		/* (2) compute text stats */
		auto block_size = t_block_size;
		size_t n = std::distance(itr,end);
		size_t num_blocks = n / block_size;
		auto left = n % block_size;
		auto blocks_per_10mib = (10*1024*1024) / block_size;

		/* (3) encode blocks */
		for(size_t i=1;i<=num_blocks;i++) {
	        auto block_end = itr + block_size;
			factorize_block<t_coder>(fs,idx,itr,block_end);
        	itr = block_end;
        	block_end += block_size;
        	if(i % blocks_per_10mib == 0) {
        		fs.output_stats(num_blocks);
        	}
		}

		/* (4) is there a non-full block? */
		if(left != 0) {
			factorize_block<t_coder>(fs,idx,itr,end);
		}

		return fs.info();
	}
};
