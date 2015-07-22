#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "experiments/rlz_types_airs.hpp"

template<uint32_t t_factorization_blocksize,uint32_t dict_size_in_bytes>
void create_indexes(collection& col,utils::cmdargs_t& args)
{
    {
        /* RLZ-ZZ */
        auto rlz_store = typename rlz_type_zz_greedy_sp<t_factorization_blocksize>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
    }
    {
        /* RLZ-ZZZ */
        using fcoder_type = factor_coder_blocked<4, coder::zlib<9>, coder::zlib<9>, coder::zlib<9>>;
        using rlz_type_zzp_greedy_sp = rlz_store_static<
                                         dict_uniform_sample_budget<airs_sample_block_size>,
                                         dict_prune_none,
                                         dict_index_csa<airs_csa_type>,
                                         t_factorization_blocksize,
                                         factor_select_first,
                                         fcoder_type,
                                         block_map_uncompressed>;
        auto rlz_store = typename rlz_type_zzp_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
    }
}

template <class t_idx>
void bench_index_rand(collection& col,const t_idx& idx,size_t dict_size_in_bytes)
{
	utils::flush_cache();

	uint64_t blocks_to_decode = 10000ULL;
	uint64_t block_ret_size = 16*1024;

	std::vector<uint64_t> byte_offsets(blocks_to_decode);
	std::mt19937 g(1234);
	std::uniform_int_distribution<uint64_t> dis(0, idx.text_size - 1 - block_ret_size);
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
              << col.path << ";"
    		  << "RAND";
}


template<uint32_t t_factorization_blocksize,uint32_t dict_size_in_bytes>
void bench_indexes_rand(collection& col,utils::cmdargs_t& args)
{
    {
        /* RLZ-ZZ */
        auto rlz_store = typename rlz_type_zz_greedy_sp<t_factorization_blocksize>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .load(col);

        bench_index_rand(col,rlz_store,dict_size_in_bytes);
    }
    {
        /* RLZ-ZZZ */
        using fcoder_type = factor_coder_blocked<4, coder::zlib<9>, coder::zlib<9>, coder::zlib<9>>;
        using rlz_type_zzp_greedy_sp = rlz_store_static<
                                         dict_uniform_sample_budget<airs_sample_block_size>,
                                         dict_prune_none,
                                         dict_index_csa<airs_csa_type>,
                                         t_factorization_blocksize,
                                         factor_select_first,
                                         fcoder_type,
                                         block_map_uncompressed>;
        auto rlz_store = typename rlz_type_zzp_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .load(col);

        bench_index_rand(col,rlz_store,dict_size_in_bytes);
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
    create_indexes<256*1024,256*1024*1024>(col,args);
    create_indexes<16*1024,256*1024*1024>(col,args);
    create_indexes<64*1024,256*1024*1024>(col,args);

    /* bench indexes */
    bench_indexes_rand<16*1024,0>(col,args);
    bench_indexes_rand<64*1024,0>(col,args);
    bench_indexes_rand<256*1024,0>(col,args);
    bench_indexes_rand<16*1024,256*1024*1024>(col,args);
    bench_indexes_rand<64*1024,256*1024*1024>(col,args);
    bench_indexes_rand<256*1024,256*1024*1024>(col,args);

    return EXIT_SUCCESS;
}