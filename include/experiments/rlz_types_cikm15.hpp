#pragma once

const uint32_t cikm_factorization_blocksize = 32768;
const uint32_t cikm_sample_block_size = 1024;

using cikm_csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;

using rlz_type_uv_greedy_sp = rlz_store_static<
								  dict_random_sample_budget<cikm_sample_block_size>,
                                  dict_prune_none,
                                  dict_index_csa<cikm_csa_type>,
                                  cikm_factorization_blocksize,
                                  factor_select_first,
                                  factor_coder_blocked<coder::u32a, coder::vbyte>,
                                  block_map_uncompressed>;

using rlz_type_zz_greedy_sp = rlz_store_static<
								  dict_random_sample_budget<cikm_sample_block_size>,
                                  dict_prune_none,
                                  dict_index_csa<cikm_csa_type>,
                                  cikm_factorization_blocksize,
                                  factor_select_first,
                                  factor_coder_blocked<coder::zlib<9>, coder::zlib<9>>,
                                  block_map_uncompressed>;

using rlz_type_zz_greedy_min = rlz_store_static<
								  dict_random_sample_budget<cikm_sample_block_size>,
                                  dict_prune_none,
                                  dict_index_csa<cikm_csa_type>,
                                  cikm_factorization_blocksize,
                                  factor_select_minimum,
                                  factor_coder_blocked<coder::zlib<9>, coder::zlib<9>>,
                                  block_map_uncompressed>;

using rlz_type_zz_lenmul2 = rlz_store_static<
								  dict_random_sample_budget<cikm_sample_block_size>,
                                  dict_prune_none,
                                  dict_index_csa_length_selector<2,cikm_csa_type>,
                                  cikm_factorization_blocksize,
                                  factor_select_first,
                                  factor_coder_blocked<coder::zlib<9>, coder::length_multiplier<2, coder::zlib<9>> >,
                                  block_map_uncompressed>;

using rlz_type_zz_lenmul22 = rlz_store_static<
								  dict_random_sample_budget<cikm_sample_block_size>,
                                  dict_prune_none,
                                  dict_index_sa_length_selector<2>,
                                  cikm_factorization_blocksize,
                                  factor_select_first,
                                  factor_coder_blocked<coder::zlib<9>, coder::length_multiplier<2, coder::zlib<9>> >,
                                  block_map_uncompressed>;

using rlz_type_zz_lenmul3 = rlz_store_static<
								  dict_random_sample_budget<cikm_sample_block_size>,
                                  dict_prune_none,
                                  dict_index_sa_length_selector<3>,
                                  cikm_factorization_blocksize,
                                  factor_select_first,
                                  factor_coder_blocked<coder::zlib<9>, coder::length_multiplier<3, coder::zlib<9>> >,
                                  block_map_uncompressed>;

