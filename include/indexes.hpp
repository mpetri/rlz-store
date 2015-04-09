#pragma once

#include "block_maps.hpp"
#include "factor_selector.hpp"
#include "factor_coder.hpp"
#include "dict_strategies.hpp"
#include "dict_indexes.hpp"

#include "rlz_store_static.hpp"

#include <sdsl/bit_vectors.hpp>
#include <sdsl/optpfor_vector.hpp>

using dict_creation_strategy = dict_random_sample_budget<32,1024>;
using dict_pruning_strategy = dict_prune_none;
using csa_type = sdsl::csa_wt< sdsl::wt_huff<sdsl::bit_vector_il<64>> , 4 , 4096 >;
using dict_index_type = dict_index_csa<csa_type>;
const uint32_t factorization_block_size = 2*2048;
using factor_selection_strategy = factor_select_first;
using factor_encoder = factor_coder<coder::vbyte,coder::vbyte>;
using block_map = block_map_uncompressed;
using rlz_type_standard = rlz_store_static<
	                        dict_creation_strategy,
	                        dict_pruning_strategy,
	                        dict_index_type,
	                        factorization_block_size,
	                        factor_selection_strategy,
	                        factor_encoder,
	                        block_map>;


/* use an uncompressed SA + LCP for factoriation instead */
using rlz_type_salcp = rlz_store_static<
	                        dict_creation_strategy,
	                        dict_pruning_strategy,
	                        dict_index_salcp,
	                        factorization_block_size,
	                        factor_selection_strategy,
	                        factor_encoder,
	                        block_map>;