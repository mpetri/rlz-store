#pragma once

#include "indexes.hpp"

#include "wt_flat.hpp"

using www_csa_type = sdsl::csa_wt<wt_flat<sdsl::bit_vector>, 1, 4096>;
const uint32_t www_uniform_sample_block_size = 1024;
const uint32_t www_factorization_blocksize = 64*1024;

//regular sampling baseline
using rlz_type_zzz_greedy_sp = rlz_store_static<dict_uniform_sample_budget<www_uniform_sample_block_size>,
                              	dict_prune_none,
                             	dict_index_csa<www_csa_type>,
                             	www_factorization_blocksize,
                             	factor_select_first,
                             	factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                             	block_map_uncompressed>;

 using rlz_type_zzz_greedy_sp_local_tb_rand = // rlz_store_static<dict_local_weighted_coverage_random_CMS<2048,16,256>,
									 // rlz_store_static<dict_local_weighted_coverage_random_CMS_topk<2048,16,256,1>,
									 rlz_store_static<dict_local_coverage_random_RS<2048,16,256, RAND>,   
									 // rlz_store_static<dict_local_coverage_random_RS_topk<1024,16,256,1>, 
									 dict_prune_none,
									 dict_index_csa<www_csa_type>,
									 www_factorization_blocksize,
									 factor_select_first,
									 factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
									 block_map_uncompressed>;

 using rlz_type_zzz_greedy_sp_local_cms_rand =  rlz_store_static<dict_local_weighted_coverage_random_CMS<2048,16,256, RAND>,
									 // rlz_store_static<dict_local_weighted_coverage_random_CMS_topk<2048,16,256,1>,
									 // rlz_store_static<dict_local_coverage_random_RS<2048,16,256, RAND>,   
									 // rlz_store_static<dict_local_coverage_random_RS_topk<1024,16,256,1>, 
									 dict_prune_none,
									 dict_index_csa<www_csa_type>,
									 www_factorization_blocksize,
									 factor_select_first,
									 factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
									 block_map_uncompressed>;

 using rlz_type_zzz_greedy_sp_local_tb_seq = // rlz_store_static<dict_local_weighted_coverage_random_CMS<2048,16,256>,
									 // rlz_store_static<dict_local_weighted_coverage_random_CMS_topk<2048,16,256,1>,
									 rlz_store_static<dict_local_coverage_random_RS<2048,16,256, SEQ>,   
									 // rlz_store_static<dict_local_coverage_random_RS_topk<1024,16,256,1>, 
									 dict_prune_none,
									 dict_index_csa<www_csa_type>,
									 www_factorization_blocksize,
									 factor_select_first,
									 factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
									 block_map_uncompressed>;

 using rlz_type_zzz_greedy_sp_local_cms_seq =  rlz_store_static<dict_local_weighted_coverage_random_CMS<2048,16,256, SEQ>,
									 // rlz_store_static<dict_local_weighted_coverage_random_CMS_topk<2048,16,256,1>,
									 // rlz_store_static<dict_local_coverage_random_RS<2048,16,256, seq>,   
									 // rlz_store_static<dict_local_coverage_random_RS_topk<1024,16,256,1>, 
									 dict_prune_none,
									 dict_index_csa<www_csa_type>,
									 www_factorization_blocksize,
									 factor_select_first,
									 factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
									 block_map_uncompressed>;
