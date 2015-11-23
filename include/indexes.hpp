#pragma once

#include "block_maps.hpp"
#include "factor_selector.hpp"
#include "factor_coder.hpp"
#include "dict_strategies.hpp"
#include "dict_indexes.hpp"

#include "rlz_store_static.hpp"
#include "rlz_store_static_builder.hpp"
#include "lz_store_static.hpp"

#include <sdsl/bit_vectors.hpp>

/* parameters */
const uint32_t default_factorization_block_size = 32768;
const uint32_t default_dict_sample_block_size = 1024;
const bool default_search_local_context = false;
using default_dict_creation_strategy = dict_uniform_sample_budget<default_dict_sample_block_size>;
using default_dict_pruning_strategy = dict_prune_none;
using default_factor_selection_strategy = factor_select_first;
using default_factor_encoder = factor_coder_blocked<1, coder::fixed<32>, coder::aligned_fixed<uint32_t>, coder::vbyte>;
using default_block_map = block_map_uncompressed;

/* dict type = csa */
using csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 4, 4096>;
using default_dict_index_type = dict_index_csa<csa_type>;

using rlz_type_standard = rlz_store_static<default_dict_creation_strategy,
                                           default_dict_pruning_strategy,
                                           default_dict_index_type,
                                           default_factorization_block_size,
                                           default_search_local_context,
                                           default_factor_selection_strategy,
                                           default_factor_encoder,
                                           default_block_map>;

template <uint32_t t_factorization_blocksize,bool t_local_search = false>
using rlz_type_u32v_greedy_sp = rlz_store_static<dict_uniform_sample_budget<default_dict_sample_block_size>,
                                                 dict_prune_none,
                                                 default_dict_index_type,
                                                 t_factorization_blocksize,
                                                 t_local_search,
                                                 factor_select_first,
                                                 factor_coder_blocked<3, coder::aligned_fixed<uint8_t>, coder::aligned_fixed<uint32_t>, coder::vbyte>,
                                                 block_map_uncompressed>;

template <uint32_t t_factorization_blocksize,bool t_local_search = false>
using rlz_type_zzz_greedy_sp = rlz_store_static<dict_uniform_sample_budget<default_dict_sample_block_size>,
                                                dict_prune_none,
                                                default_dict_index_type,
                                                t_factorization_blocksize,
                                                t_local_search,
                                                factor_select_first,
                                                factor_coder_blocked<3, coder::zlib<9>, coder::zlib<9>, coder::zlib<9> >,
                                                block_map_uncompressed>;
