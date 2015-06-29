#pragma once

#include "indexes.hpp"

using airs_csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
const uint32_t airs_sample_block_size = 1024;

template<uint32_t t_factorization_blocksize>
using rlz_type_u32v_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                             dict_prune_none,
                             dict_index_csa<airs_csa_type>,
                             t_factorization_blocksize,
                             factor_select_first,
                             factor_coder_blocked_twostream<1,coder::aligned_fixed<uint32_t>,coder::vbyte>,
                             block_map_uncompressed>;

template<uint32_t t_factorization_blocksize>
using rlz_type_zz_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                             dict_prune_none,
                             dict_index_csa<airs_csa_type>,
                             t_factorization_blocksize,
                             factor_select_first,
                             factor_coder_blocked_twostream<1,coder::zlib<9>,coder::zlib<9>>,
                             block_map_uncompressed>;

template<uint32_t t_factorization_blocksize>
using rlz_type_ll_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                             dict_prune_none,
                             dict_index_csa<airs_csa_type>,
                             t_factorization_blocksize,
                             factor_select_first,
                             factor_coder_blocked_twostream<1,coder::lz4hc<16>,coder::lz4hc<16>>,
                             block_map_uncompressed>;