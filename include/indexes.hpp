#pragma once

#include "block_maps.hpp"
#include "factor_selector.hpp"
#include "factor_coder.hpp"
#include "dict_strategies.hpp"
#include "dict_indexes.hpp"

#include "rlz_store_static.hpp"
#include "rlz_store_static_builder.hpp"

#include <sdsl/bit_vectors.hpp>
#include <sdsl/optpfor_vector.hpp>

/* parameters */
const uint32_t default_factorization_block_size = 32768;
using default_dict_creation_strategy = dict_random_sample_budget<1024>;
using default_dict_pruning_strategy = dict_prune_none;
using default_factor_selection_strategy = factor_select_first;
using default_factor_encoder = factor_coder<coder::u32, coder::vbyte>;
using default_block_map = block_map_uncompressed;

/* dict type = csa */
using csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 4, 4096>;
using default_dict_index_type = dict_index_csa<csa_type>;

using rlz_type_standard = rlz_store_static<default_dict_creation_strategy,
                                           default_dict_pruning_strategy,
                                           default_dict_index_type,
                                           default_factorization_block_size,
                                           default_factor_selection_strategy,
                                           default_factor_encoder,
                                           default_block_map>;

