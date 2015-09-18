#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

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
    	/* create the default dict */
        const uint32_t block_size = 32768;
        using csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
        
        //baseline 1
        using rlz_type_zzz_greedy_sp_regular = rlz_store_static<dict_uniform_sample_budget<1024>,
                                     dict_prune_none,
                                     dict_index_csa<csa_type>,
                                     block_size,
                                     factor_select_first,
                                     factor_coder_blocked<1,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
        auto rlz_store_base1 = typename rlz_type_zzz_greedy_sp_regular::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(32*1024*1024ULL)
                             // .set_dict_size(512*1024*1024ULL)
                             .build_or_load(col);

        //baseline 2
        using rlz_type_zzz_greedy_sp_prune = rlz_store_static<dict_uniform_sample_budget<1024>,
                                     dict_prune_care<10,20,FFT>,
                                     dict_index_csa<csa_type>,
                                     block_size,
                                     factor_select_first,
                                     factor_coder_blocked<1,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
        auto rlz_store_base2 = typename rlz_type_zzz_greedy_sp_prune::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(128*1024*1024ULL)
                             //.set_dict_size(2048*1024*1024ULL)
                             .set_pruned_dict_size(32*1024*1024ULL)
                             .build_or_load(col);
        //next indices                     
        using rlz_type_zzz_greedy_sp_new1 = 
                                     // rlz_store_static<dict_local_weighted_coverage_random_CMS<2048,16,256>,
                                     rlz_store_static<dict_local_weighted_coverage_random_CMS_topk<2048,16,256,1>,
                                     // rlz_store_static<dict_local_coverage_random_RS<2048,16,256>,   
                                     // rlz_store_static<dict_local_coverage_random_RS_topk<1024,16,256,1>, 
                                     dict_prune_none,
                                     dict_index_csa<csa_type>,
                                     block_size,
                                     factor_select_first,
                                     factor_coder_blocked<1,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                     block_map_uncompressed>;
        auto rlz_store_new1 = typename rlz_type_zzz_greedy_sp_new1::builder{}
                             .set_rebuild(true)
                             .set_threads(args.threads)
                             .set_dict_size(32*1024*1024ULL)
                             //.set_dict_size(512*1024*1024ULL)
                             .build_or_load(col);

        // compare_indexes(col,rlz_store_base1,rlz_store_base2,rlz_store_new1);
        
    }

    return EXIT_SUCCESS;
}
