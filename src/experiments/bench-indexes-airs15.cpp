#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "experiments/rlz_types_airs.hpp"

using namespace std::chrono;

template<size_t N, size_t P = 0>
constexpr typename std::enable_if<(N <= 1), size_t>::type CLog2()
{
   return P;
}

template<size_t N, size_t P = 0>
constexpr typename std::enable_if<!(N <= 1), size_t>::type CLog2()
{
   return CLog2<N / 2, P + 1>();
}


template <class t_idx>
void bench_index_full(const t_idx& idx,size_t dict_size_in_bytes)
{
	utils::flush_cache();

	auto start = hrclock::now();

    auto itr = idx.begin();
    auto end = idx.end();

    size_t checksum = 0;
    size_t num_syms = 0;
    auto n = idx.size();
    for (size_t i = 0; i < n; i++) {
        checksum += *itr;
        num_syms++;
        ++itr;
    }
    auto stop = hrclock::now();
    if (checksum == 0) {
        LOG(ERROR) << "text decoding speed checksum error";
    }

    auto time_ms = duration_cast<milliseconds>(stop - start);
    auto num_blocks = num_syms / t_idx::block_size;
    LOG(INFO) << idx.type() << ";"
    		  << dict_size_in_bytes << ";"
    		  << idx.encoding_block_size << ";"
    		  << time_ms.count() << ";"
    		  << num_syms << ";"
    		  << num_blocks << ";"
    		  << checksum << ";"
              << idx.size_in_bytes() << ";"
              << idx.size() << ";"
    		  << "FULL";
}

template <class t_idx>
void bench_index_rand_aligned(const t_idx& idx,size_t dict_size_in_bytes)
{
	utils::flush_cache();

	uint64_t blocks_to_decode = 10000ULL;
	uint64_t block_ret_size = 16*1024;
	auto total_ret_blocks = idx.text_size / block_ret_size;


	std::vector<uint32_t> block_ids(total_ret_blocks);
	for(size_t i=0;i<total_ret_blocks;i++) {
		block_ids[i] = i;
	}
	std::mt19937 g(1234);
	std::shuffle(block_ids.begin(),block_ids.end(), g);
	block_ids.resize(blocks_to_decode);

	std::vector<uint8_t> block_content(idx.encoding_block_size);
	block_factor_data bfd(idx.encoding_block_size);

	auto itr = idx.begin();
	auto start = hrclock::now();
	size_t checksum = 0;
	size_t num_syms = 0;
	for(const auto bid: block_ids) {
		auto text_ret_offset = bid*block_ret_size;
		itr.seek(text_ret_offset);
		num_syms += block_ret_size;
		for (size_t i = 0; i < block_ret_size; i++) {
			checksum += *itr;
			++itr;
		}
	}
    auto stop = hrclock::now();
    if (checksum == 0) {
        LOG(ERROR) << "text decoding speed checksum error";
    }

    auto time_ms = duration_cast<milliseconds>(stop - start);
    LOG(INFO) << idx.type() << ";"
    		  << dict_size_in_bytes << ";"
    		  << idx.encoding_block_size << ";"
    		  << time_ms.count() << ";"
    		  << num_syms << ";"
    		  << blocks_to_decode << ";"
    		  << block_ret_size << ";"
    		  << checksum << ";"
              << idx.size_in_bytes() << ";"
              << idx.size() << ";"
    		  << "RAND-ALIGNED";
}

template <class t_idx>
void bench_index_rand(const t_idx& idx,size_t dict_size_in_bytes)
{
	utils::flush_cache();

	uint64_t blocks_to_decode = 10000ULL;
	uint64_t block_ret_size = 16*1024;

	std::vector<uint32_t> byte_offsets(blocks_to_decode);
	std::mt19937 g(1234);
	std::uniform_int_distribution<> dis(0, idx.text_size - 1);
	for(size_t i=0;i<blocks_to_decode;i++) {
		byte_offsets[i] = dis(g);
	}
	
	std::vector<uint8_t> block_content(idx.encoding_block_size);
	block_factor_data bfd(idx.encoding_block_size);

	auto itr = idx.begin();
	auto start = hrclock::now();
	size_t checksum = 0;
	size_t num_syms = 0;
	for(const auto bo: byte_offsets) {
		auto text_ret_offset = bo;
		itr.seek(text_ret_offset);
		num_syms += block_ret_size;
		for (size_t i = 0; i < block_ret_size; i++) {
			checksum += *itr;
			++itr;
		}
	}
    auto stop = hrclock::now();
    if (checksum == 0) {
        LOG(ERROR) << "text decoding speed checksum error";
    }

    auto time_ms = duration_cast<milliseconds>(stop - start);
    LOG(INFO) << idx.type() << ";"
    		  << dict_size_in_bytes << ";"
    		  << idx.encoding_block_size << ";"
    		  << time_ms.count() << ";"
    		  << num_syms << ";"
    		  << blocks_to_decode << ";"
    		  << block_ret_size << ";"
    		  << checksum << ";"
              << idx.size_in_bytes() << ";"
              << idx.size() << ";"
    		  << "RAND";
}


template <class t_idx>
void bench_index_batch(const t_idx& idx,size_t dict_size_in_bytes)
{
	utils::flush_cache();

	uint64_t blocks_to_decode = 10000;
	uint64_t block_ret_size = 16*1024;
	auto total_ret_blocks = idx.text_size / block_ret_size;

	std::vector<uint32_t> block_ids(total_ret_blocks);
	for(size_t i=0;i<total_ret_blocks;i++) {
		block_ids[i] = i;
	}
	std::mt19937 g(1234);
	std::shuffle(block_ids.begin(),block_ids.end(), g);
	block_ids.resize(blocks_to_decode);
	std::sort(block_ids.begin(),block_ids.end());

	std::vector<uint8_t> block_content(idx.encoding_block_size);
	block_factor_data bfd(idx.encoding_block_size);

	auto itr = idx.begin();
	auto start = hrclock::now();
	size_t checksum = 0;
	size_t num_syms = 0;
	for(const auto bid: block_ids) {
		auto text_ret_offset = bid*block_ret_size;
		itr.seek(text_ret_offset);
		num_syms += block_ret_size;
		for (size_t i = 0; i < block_ret_size; i++) {
			checksum += *itr;
			++itr;
		}
	}
    auto stop = hrclock::now();
    if (checksum == 0) {
        LOG(ERROR) << "text decoding speed checksum error";
    }

    auto time_ms = duration_cast<milliseconds>(stop - start);
    LOG(INFO) << idx.type() << ";"
    		  << dict_size_in_bytes << ";"
    		  << idx.encoding_block_size << ";"
    		  << time_ms.count() << ";"
    		  << num_syms << ";"
    		  << blocks_to_decode << ";"
    		  << block_ret_size << ";"
    		  << checksum << ";"
              << idx.size_in_bytes() << ";"
              << idx.size() << ";"
    		  << "BATCH";
}

template<uint32_t t_factorization_blocksize,uint32_t dict_size_in_bytes>
void bench_indexes_full(collection& col,utils::cmdargs_t& args)
{
	/* raw compression */
	if(dict_size_in_bytes == 0) {
	    // {
	    //     auto lz_store = typename lz_store_static<coder::bzip2<9>,t_factorization_blocksize>::builder{}
	    //                          .set_rebuild(args.rebuild)
	    //                          .set_threads(args.threads)
	    //                          .set_dict_size(dict_size_in_bytes)
	    //                          .load(col);

	    //     bench_index_rand_aligned(lz_store,dict_size_in_bytes);
	    // }
	    {
	        auto lz_store = typename lz_store_static<coder::zlib<9>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_full(lz_store,dict_size_in_bytes);
	    }
	    {
	        auto lz_store = typename lz_store_static<coder::lz4hc<16>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_full(lz_store,dict_size_in_bytes);
	    }
	    {
	        auto lz_store = typename lz_store_static<coder::fixed<8>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_full(lz_store,dict_size_in_bytes);
	    }
	} else {
	    /* rlz compression */
	    using airs_csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
	    const uint32_t airs_sample_block_size = 1024;
	    {
	    	/* RLZ-UV 32bit literals*/
        	const uint32_t dict_size_in_bytes_log2 = CLog2<dict_size_in_bytes>();
    		/* RLZ-UV LOG(D)bit offsets */
        	using rlz_type_uv_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<airs_csa_type>,
                                     t_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked_twostream<1,coder::fixed<dict_size_in_bytes_log2>,coder::vbyte>,
                                     block_map_uncompressed>;
	        auto rlz_store = typename rlz_type_uv_greedy_sp::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_full(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-U32V */
	        auto rlz_store = typename rlz_type_u32v_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_full(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-ZZ */
	        auto rlz_store = typename rlz_type_zz_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_full(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-LL */
	        auto rlz_store = typename rlz_type_ll_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_full(rlz_store,dict_size_in_bytes);
	    }
	}
}

template<uint32_t t_factorization_blocksize,uint32_t dict_size_in_bytes>
void bench_indexes_batch(collection& col,utils::cmdargs_t& args)
{
	/* raw compression */
	if(dict_size_in_bytes == 0) {
	    // {
	    //     auto lz_store = typename lz_store_static<coder::bzip2<9>,t_factorization_blocksize>::builder{}
	    //                          .set_rebuild(args.rebuild)
	    //                          .set_threads(args.threads)
	    //                          .set_dict_size(dict_size_in_bytes)
	    //                          .load(col);

	    //     bench_index_rand_aligned(lz_store,dict_size_in_bytes);
	    // }
	    {
	        auto lz_store = typename lz_store_static<coder::zlib<9>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_batch(lz_store,dict_size_in_bytes);
	    }
	    {
	        auto lz_store = typename lz_store_static<coder::lz4hc<16>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_batch(lz_store,dict_size_in_bytes);
	    }
	    {
	        auto lz_store = typename lz_store_static<coder::fixed<8>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_batch(lz_store,dict_size_in_bytes);
	    }
	} else {
	    /* rlz compression */
	    using airs_csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
	    const uint32_t airs_sample_block_size = 1024;
	    {
	    	/* RLZ-UV 32bit literals*/
        	const uint32_t dict_size_in_bytes_log2 = CLog2<dict_size_in_bytes>();
    		/* RLZ-UV LOG(D)bit offsets */
        	using rlz_type_uv_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<airs_csa_type>,
                                     t_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked_twostream<1,coder::fixed<dict_size_in_bytes_log2>,coder::vbyte>,
                                     block_map_uncompressed>;
	        auto rlz_store = typename rlz_type_uv_greedy_sp::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_batch(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-U32V */
	        auto rlz_store = typename rlz_type_u32v_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_batch(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-ZZ */
	        auto rlz_store = typename rlz_type_zz_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_batch(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-LL */
	        auto rlz_store = typename rlz_type_ll_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_batch(rlz_store,dict_size_in_bytes);
	    }
	}
}


template<uint32_t t_factorization_blocksize,uint32_t dict_size_in_bytes>
void bench_indexes_rand(collection& col,utils::cmdargs_t& args)
{
	/* raw compression */
	if(dict_size_in_bytes == 0) {
	    // {
	    //     auto lz_store = typename lz_store_static<coder::bzip2<9>,t_factorization_blocksize>::builder{}
	    //                          .set_rebuild(args.rebuild)
	    //                          .set_threads(args.threads)
	    //                          .set_dict_size(dict_size_in_bytes)
	    //                          .load(col);

	    //     bench_index_rand_aligned(lz_store,dict_size_in_bytes);
	    // }
	    {
	        auto lz_store = typename lz_store_static<coder::zlib<9>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand(lz_store,dict_size_in_bytes);
	    }
	    {
	        auto lz_store = typename lz_store_static<coder::lz4hc<16>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand(lz_store,dict_size_in_bytes);
	    }
	    {
	        auto lz_store = typename lz_store_static<coder::fixed<8>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand(lz_store,dict_size_in_bytes);
	    }
	} else {
	    /* rlz compression */
	    using airs_csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
	    const uint32_t airs_sample_block_size = 1024;
	    {
	    	/* RLZ-UV 32bit literals*/
        	const uint32_t dict_size_in_bytes_log2 = CLog2<dict_size_in_bytes>();
    		/* RLZ-UV LOG(D)bit offsets */
        	using rlz_type_uv_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<airs_csa_type>,
                                     t_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked_twostream<1,coder::fixed<dict_size_in_bytes_log2>,coder::vbyte>,
                                     block_map_uncompressed>;
	        auto rlz_store = typename rlz_type_uv_greedy_sp::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-U32V */
	        auto rlz_store = typename rlz_type_u32v_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-ZZ */
	        auto rlz_store = typename rlz_type_zz_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-LL */
	        auto rlz_store = typename rlz_type_ll_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand(rlz_store,dict_size_in_bytes);
	    }
	}
}


template<uint32_t t_factorization_blocksize,uint32_t dict_size_in_bytes>
void bench_indexes_rand_aligned(collection& col,utils::cmdargs_t& args)
{
	/* raw compression */
	if(dict_size_in_bytes == 0) {
	    // {
	    //     auto lz_store = typename lz_store_static<coder::bzip2<9>,t_factorization_blocksize>::builder{}
	    //                          .set_rebuild(args.rebuild)
	    //                          .set_threads(args.threads)
	    //                          .set_dict_size(dict_size_in_bytes)
	    //                          .load(col);

	    //     bench_index_rand_aligned(lz_store,dict_size_in_bytes);
	    // }
	    {
	        auto lz_store = typename lz_store_static<coder::zlib<9>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand_aligned(lz_store,dict_size_in_bytes);
	    }
	    {
	        auto lz_store = typename lz_store_static<coder::lz4hc<16>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand_aligned(lz_store,dict_size_in_bytes);
	    }
	    {
	        auto lz_store = typename lz_store_static<coder::fixed<8>,t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand_aligned(lz_store,dict_size_in_bytes);
	    }
	} else {
	    /* rlz compression */
	    using airs_csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
	    const uint32_t airs_sample_block_size = 1024;
	    {
	    	/* RLZ-UV 32bit literals*/
        	const uint32_t dict_size_in_bytes_log2 = CLog2<dict_size_in_bytes>();
    		/* RLZ-UV LOG(D)bit offsets */
        	using rlz_type_uv_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<airs_csa_type>,
                                     t_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked_twostream<1,coder::fixed<dict_size_in_bytes_log2>,coder::vbyte>,
                                     block_map_uncompressed>;
	        auto rlz_store = typename rlz_type_uv_greedy_sp::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand_aligned(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-U32V */
	        auto rlz_store = typename rlz_type_u32v_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand_aligned(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-ZZ */
	        auto rlz_store = typename rlz_type_zz_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand_aligned(rlz_store,dict_size_in_bytes);
	    }
	    {
	    	/* RLZ-LL */
	        auto rlz_store = typename rlz_type_ll_greedy_sp<t_factorization_blocksize>::builder{}
	                             .set_rebuild(args.rebuild)
	                             .set_threads(args.threads)
	                             .set_dict_size(dict_size_in_bytes)
	                             .load(col);

	        bench_index_rand_aligned(rlz_store,dict_size_in_bytes);
	    }
	}
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    auto args = utils::parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* create rlz index */
    {
	    bench_indexes_rand_aligned<4*1024,0>(col,args);
	    bench_indexes_rand_aligned<16*1024,0>(col,args);
	    bench_indexes_rand_aligned<64*1024,0>(col,args);

	    bench_indexes_rand_aligned<4*1024,4*1024*1024>(col,args);
	    bench_indexes_rand_aligned<16*1024,4*1024*1024>(col,args);
	    bench_indexes_rand_aligned<64*1024,4*1024*1024>(col,args);

	    bench_indexes_rand_aligned<4*1024,16*1024*1024>(col,args);
	    bench_indexes_rand_aligned<16*1024,16*1024*1024>(col,args);
	    bench_indexes_rand_aligned<64*1024,16*1024*1024>(col,args);

	    bench_indexes_rand_aligned<4*1024,64*1024*1024>(col,args);
	    bench_indexes_rand_aligned<16*1024,64*1024*1024>(col,args);
	    bench_indexes_rand_aligned<64*1024,64*1024*1024>(col,args);
	}
    {
	    bench_indexes_rand<4*1024,0>(col,args);
	    bench_indexes_rand<16*1024,0>(col,args);
	    bench_indexes_rand<64*1024,0>(col,args);

	    bench_indexes_rand<4*1024,4*1024*1024>(col,args);
	    bench_indexes_rand<16*1024,4*1024*1024>(col,args);
	    bench_indexes_rand<64*1024,4*1024*1024>(col,args);

	    bench_indexes_rand<4*1024,16*1024*1024>(col,args);
	    bench_indexes_rand<16*1024,16*1024*1024>(col,args);
	    bench_indexes_rand<64*1024,16*1024*1024>(col,args);

	    bench_indexes_rand<4*1024,64*1024*1024>(col,args);
	    bench_indexes_rand<16*1024,64*1024*1024>(col,args);
	    bench_indexes_rand<64*1024,64*1024*1024>(col,args);
	}
    {
	    bench_indexes_batch<4*1024,0>(col,args);
	    bench_indexes_batch<16*1024,0>(col,args);
	    bench_indexes_batch<64*1024,0>(col,args);

	    bench_indexes_batch<4*1024,4*1024*1024>(col,args);
	    bench_indexes_batch<16*1024,4*1024*1024>(col,args);
	    bench_indexes_batch<64*1024,4*1024*1024>(col,args);

	    bench_indexes_batch<4*1024,16*1024*1024>(col,args);
	    bench_indexes_batch<16*1024,16*1024*1024>(col,args);
	    bench_indexes_batch<64*1024,16*1024*1024>(col,args);

	    bench_indexes_rand<4*1024,64*1024*1024>(col,args);
	    bench_indexes_rand<16*1024,64*1024*1024>(col,args);
	    bench_indexes_rand<64*1024,64*1024*1024>(col,args);
	}
    {
	    bench_indexes_full<4*1024,0>(col,args);
	    bench_indexes_full<16*1024,0>(col,args);
	    bench_indexes_full<64*1024,0>(col,args);

	    bench_indexes_full<4*1024,4*1024*1024>(col,args);
	    bench_indexes_full<16*1024,4*1024*1024>(col,args);
	    bench_indexes_full<64*1024,4*1024*1024>(col,args);

	    bench_indexes_full<4*1024,16*1024*1024>(col,args);
	    bench_indexes_full<16*1024,16*1024*1024>(col,args);
	    bench_indexes_full<64*1024,16*1024*1024>(col,args);

	    bench_indexes_full<4*1024,64*1024*1024>(col,args);
	    bench_indexes_full<16*1024,64*1024*1024>(col,args);
	    bench_indexes_full<64*1024,64*1024*1024>(col,args);
	}


    return EXIT_SUCCESS;
}
