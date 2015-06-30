#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "experiments/rlz_types_airs.hpp"

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

template<uint32_t t_factorization_blocksize,uint32_t dict_size_in_bytes>
void create_indexes(collection& col,utils::cmdargs_t& args)
{
	/* raw compression */
    if(dict_size_in_bytes == 0) {
        // {
        //     auto lz_store = typename lz_store_static<coder::bzip2<9>,t_factorization_blocksize>::builder{}
        //                          .set_rebuild(args.rebuild)
        //                          .set_threads(args.threads)
        //                          .set_dict_size(dict_size_in_bytes)
        //                          .build_or_load(col);

        //     if(args.verify) verify_index(col, lz_store);
        // }
        {
            auto lz_store = typename lz_store_static<coder::zlib<9>,t_factorization_blocksize>::builder{}
                                 .set_rebuild(args.rebuild)
                                 .set_threads(args.threads)
                                 .set_dict_size(dict_size_in_bytes)
                                 .build_or_load(col);

            if(args.verify) verify_index(col, lz_store);
        }
        {
            auto lz_store = typename lz_store_static<coder::lz4hc<16>,t_factorization_blocksize>::builder{}
                                 .set_rebuild(args.rebuild)
                                 .set_threads(args.threads)
                                 .set_dict_size(dict_size_in_bytes)
                                 .build_or_load(col);

            if(args.verify) verify_index(col, lz_store);
        }
        {
            auto lz_store = typename lz_store_static<coder::fixed<8>,t_factorization_blocksize>::builder{}
                                 .set_rebuild(args.rebuild)
                                 .set_threads(args.threads)
                                 .set_dict_size(dict_size_in_bytes)
                                 .build_or_load(col);

            if(args.verify) verify_index(col, lz_store);
        }
        return;
    }
    /* rlz compression */
    {
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
                             .build_or_load(col);

        if(args.verify) verify_index(col, rlz_store);
    }
    {
    	/* RLZ-U32V  */
        auto rlz_store = typename rlz_type_u32v_greedy_sp<t_factorization_blocksize>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, rlz_store);
    }
    {
    	/* RLZ-ZZ */
        auto rlz_store = typename rlz_type_zz_greedy_sp<t_factorization_blocksize>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, rlz_store);
    }
    {
    	/* RLZ-LLL */
        auto rlz_store = typename rlz_type_ll_greedy_sp<t_factorization_blocksize>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, rlz_store);
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
    create_indexes<4*1024,4*1024*1024>(col,args);
    create_indexes<16*1024,4*1024*1024>(col,args);
    create_indexes<64*1024,4*1024*1024>(col,args);

    create_indexes<4*1024,16*1024*1024>(col,args);
    create_indexes<16*1024,16*1024*1024>(col,args);
    create_indexes<64*1024,16*1024*1024>(col,args);

    create_indexes<4*1024,64*1024*1024>(col,args);
    create_indexes<16*1024,64*1024*1024>(col,args);
    create_indexes<64*1024,64*1024*1024>(col,args);

    create_indexes<4*1024,0*1024*1024>(col,args);
    create_indexes<16*1024,0*1024*1024>(col,args);
    create_indexes<64*1024,0*1024*1024>(col,args);

    return EXIT_SUCCESS;
}