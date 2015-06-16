#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template<uint32_t t_factorization_blocksize>
void create_indexes(collection& col,utils::cmdargs_t& args,uint32_t dict_size_in_bytes)
{
	/* raw compression */
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
        auto lz_store = typename lz_store_static<coder::lzma<6>,t_factorization_blocksize>::builder{}
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
    /* rlz compression */
    using airs_csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
    const uint32_t airs_sample_block_size = 1024;
    {
    	/* RLZ-UV 32bit literals*/
        using rlz_type_uv_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<airs_csa_type>,
                                     t_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<1,coder::fixed<32>,coder::aligned_fixed<uint32_t>,coder::vbyte>,
                                     block_map_uncompressed>;
        auto rlz_store = typename rlz_type_uv_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, rlz_store);
    }
    {
    	/* RLZ-UUV 8bit literals*/
        using rlz_type_uuv_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<airs_csa_type>,
                                     t_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<1,coder::aligned_fixed<uint8_t>,coder::aligned_fixed<uint32_t>,coder::vbyte>,
                                     block_map_uncompressed>;
        auto rlz_store = typename rlz_type_uuv_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, rlz_store);
    }
    {
    	/* RLZ-ZZZ */
        using rlz_type_zzz_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<airs_csa_type>,
                                     t_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<1,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
        auto rlz_store = typename rlz_type_zzz_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, rlz_store);
    }
    {
    	/* RLZ-LLL */
        using rlz_type_lll_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<airs_csa_type>,
                                     t_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<1,coder::lz4hc<16>,coder::lz4hc<16>,coder::lz4hc<16>>,
                                     block_map_uncompressed>;
        auto rlz_store = typename rlz_type_lll_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, rlz_store);
    }
    // {
    // 	/* RLZ-XXX */
    //     using rlz_type_xxx_greedy_sp = rlz_store_static<dict_uniform_sample_budget<airs_sample_block_size>,
    //                                  dict_prune_none,
    //                                  dict_index_csa<airs_csa_type>,
    //                                  t_factorization_blocksize,
    //                                  factor_select_first,
    //                                  factor_coder_blocked<1,coder::lzma<6>,coder::lzma<6>,coder::lzma<6>>,
    //                                  block_map_uncompressed>;
    //     auto rlz_store = typename 
    //     rlz_type_xxx_greedy_sp::builder{}
    //                          .set_rebuild(args.rebuild)
    //                          .set_threads(args.threads)
    //                          .set_dict_size(dict_size_in_bytes)
    //                          .build_or_load(col);

    //     if(args.verify) verify_index(col, rlz_store);
    // }
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
    std::vector<uint32_t> dict_sizes{128,64,16,4};
    for(auto ds_mb : dict_sizes) {
	    create_indexes<1024>(col,args,ds_mb*1024*1024);
	    create_indexes<2*1024>(col,args,ds_mb*1024*1024);
	    create_indexes<4*1024>(col,args,ds_mb*1024*1024);
	    create_indexes<8*1024>(col,args,ds_mb*1024*1024);
	    create_indexes<16*1024>(col,args,ds_mb*1024*1024);
	    create_indexes<32*1024>(col,args,ds_mb*1024*1024);
	    create_indexes<64*1024>(col,args,ds_mb*1024*1024);
	    create_indexes<128*1024>(col,args,ds_mb*1024*1024);
	}

    return EXIT_SUCCESS;
}