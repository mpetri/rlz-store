#pragma once

#include "block_maps.hpp"
#include "factor_selector.hpp"
#include "factor_strategies.hpp"
#include "factor_coder.hpp"
#include "dict_strategies.hpp"

#include "rlz_store_static.hpp"

#include <sdsl/bit_vectors.hpp>
#include <sdsl/optpfor_vector.hpp>

const uint32_t factorization_block_size = 2*2048;
using dict_creation_strategy = dict_random_sample_budget<32,1024>;
using dict_pruning_strategy = dict_prune_none;
using csa_type = sdsl::csa_wt< sdsl::wt_huff<sdsl::bit_vector_il<64>> , 4 , 4096 >;
using factor_selection_strategy = factor_select_first;
using factorization_strategy = factor_strategy_greedy_backward_search<factorization_block_size,csa_type,factor_selection_strategy>;
using factor_encoder = factor_coder<coder::vbyte,coder::vbyte>;
using block_map = block_map_uncompressed;
using rlz_type_standard = rlz_store_static<
	                        dict_creation_strategy,
	                        dict_pruning_strategy,
	                        factorization_strategy,
	                        factor_encoder,
	                        block_map>;