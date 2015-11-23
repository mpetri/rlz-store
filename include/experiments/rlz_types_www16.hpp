#pragma once

#include "indexes.hpp"

#include "wt_flat.hpp"

using www_csa_type = sdsl::csa_wt<wt_flat<sdsl::bit_vector>, 1, 4096>;
const uint32_t www_uniform_sample_block_size = 1024;
const uint32_t www_factorization_blocksize = 64*1024;

//regular sampling baseline
using rlz_type_zzz_greedy_sp_4 = rlz_store_static<dict_uniform_sample_budget<1024*4>,
                                    dict_prune_none,
                                    dict_index_csa<www_csa_type>,
                                    www_factorization_blocksize,
                                    factor_select_first,
                                    factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                    block_map_uncompressed>;

using rlz_type_zzz_greedy_sp_8 = rlz_store_static<dict_uniform_sample_budget<1024*8>,
                                    dict_prune_none,
                                    dict_index_csa<www_csa_type>,
                                    www_factorization_blocksize,
                                    factor_select_first,
                                    factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                    block_map_uncompressed>;

using rlz_type_zzz_greedy_sp_16 = rlz_store_static<dict_uniform_sample_budget<1024*16>,
                                    dict_prune_none,
                                    dict_index_csa<www_csa_type>,
                                    www_factorization_blocksize,
                                    factor_select_first,
                                    factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                    block_map_uncompressed>;



using rlz_type_zz_greedy_sp_org = rlz_store_static<dict_uniform_sample_budget<www_uniform_sample_block_size>,
                                dict_prune_none,
                                dict_index_csa<www_csa_type>,
                                www_factorization_blocksize,
                                factor_select_first,
				factor_coder_blocked_twostream<1,coder::zlib<9>,coder::zlib<9>>,
                                block_map_uncompressed>;

//CARE + regular sampling
 using rlz_type_zzz_greedy_sp_care_regsamp =  rlz_store_static<dict_uniform_sample_budget<1024>,
                                                                         dict_prune_care<10,20,FFT>,
                                                                         dict_index_csa<www_csa_type>,
                                                                         www_factorization_blocksize,
                                                                         factor_select_first,
                                                                         factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                                                        block_map_uncompressed>;
//CARE + local_cms
 using rlz_type_zzz_greedy_sp_care_local_cms =  rlz_store_static<dict_local_weighted_coverage_random_CMS<2048,16,256, RAND>,
                                                                         dict_prune_care<10,20,FFT>,
                                                                         dict_index_csa<www_csa_type>,
                                                                         www_factorization_blocksize,
                                                                         factor_select_first,
                                                                         factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                                                        block_map_uncompressed>;
//norms
using rlz_type_zzz_greedy_sp_local_zero_norm_rand =  rlz_store_static<dict_local_coverage_norms<2048,16,256, std::ratio<0,1>, RAND>, 
                                     dict_prune_none,
				     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
				     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;

using rlz_type_zzz_greedy_sp_local_half_norm_rand =  rlz_store_static<dict_local_coverage_norms<1024,16,256, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;

using rlz_type_zzz_greedy_sp_local_half_norm_seq =  rlz_store_static<dict_local_coverage_norms<2048,16,256, std::ratio<1,2>, SEQ>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;

using rlz_type_zzz_greedy_sp_local_one_norm_rand =  rlz_store_static<dict_local_coverage_norms<2048,16,256, std::ratio<1,1>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;

using rlz_type_zzz_greedy_sp_local_onehalf_norm_rand =  rlz_store_static<dict_local_coverage_norms<2048,16,256, std::ratio<3,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;


//para: big block sizes, with half norm
using rlz_type_zzz_greedy_sp_local_half_norm_rand_size1 =  rlz_store_static<dict_local_coverage_norms<1024,16,256, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;

using rlz_type_zzz_greedy_sp_local_half_norm_rand_size4 =  rlz_store_static<dict_local_coverage_norms<4096,16,256, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;

using rlz_type_zzz_greedy_sp_local_half_norm_rand_size8 =  rlz_store_static<dict_local_coverage_norms<8192,16,256, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
//para: rs down sizes, with half norm
using rlz_type_zzz_greedy_sp_local_half_norm_rand_downsize64 =  rlz_store_static<dict_local_coverage_norms<2048,16,64, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
using rlz_type_zzz_greedy_sp_local_half_norm_rand_downsize128 =  rlz_store_static<dict_local_coverage_norms<2048,16,128, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
using rlz_type_zzz_greedy_sp_local_half_norm_rand_downsize512 =  rlz_store_static<dict_local_coverage_norms<2048,16,512, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
using rlz_type_zzz_greedy_sp_local_half_norm_rand_downsize1024 =  rlz_store_static<dict_local_coverage_norms<2048,16,1024, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
using rlz_type_zzz_greedy_sp_local_half_norm_rand_downsize2048 =  rlz_store_static<dict_local_coverage_norms<2048,16,2048, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
using rlz_type_zzz_greedy_sp_local_half_norm_rand_downsize4096 =  rlz_store_static<dict_local_coverage_norms<2048,16,4096, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;

//CARE + local_0.5norm
 using rlz_type_zzz_greedy_sp_care_local_half_norm =  rlz_store_static<dict_local_coverage_norms<2048,16,256, std::ratio<1,2>, RAND>,
                                                                         dict_prune_care<10,20,FFT>,
                                                                         dict_index_csa<www_csa_type>,
                                                                         www_factorization_blocksize,
                                                                         factor_select_first,
                                                                         factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                                                        block_map_uncompressed>;


//new assembly strategy
using rlz_type_zzz_greedy_sp_assembly =  rlz_store_static<dict_assemble_gsc<2048,16,256, std::ratio<1,2>, RAND>,
                                     dict_prune_none,
                                     dict_index_csa<www_csa_type>,
                                     www_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<3,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
