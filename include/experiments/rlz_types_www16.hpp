#pragma once

#include "indexes.hpp"

#include "wt_flat.hpp"

using www_csa_type = sdsl::csa_wt<wt_flat<sdsl::bit_vector>, 1, 4096>;
const uint32_t www_uniform_sample_block_size = 1024;
const uint32_t www_factorization_blocksize = 64*1024;

using rlz_type_zzz_greedy_sp = rlz_store_static<dict_uniform_sample_budget<www_uniform_sample_block_size>,
                              	dict_prune_none,
                             	dict_index_csa<www_csa_type>,
                             	www_factorization_blocksize,
                             	factor_select_first,
                             	factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                             	block_map_uncompressed>;
