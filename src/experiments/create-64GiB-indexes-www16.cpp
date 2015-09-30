#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "experiments/rlz_types_www16.hpp"


template<uint32_t dict_size_in_bytes>
void create_indexes(collection& col,utils::cmdargs_t& args)
{
    {
    	/* RLZ-ZZ */
 /*       
        auto rlz_store_0 = rlz_type_zz_greedy_sp::builder{}
                                 .set_rebuild(args.rebuild)
                                 .set_threads(args.threads)
                                 .set_dict_size(dict_size_in_bytes)
                                 .build_or_load(col);

    	compare_indexes(col,rlz_store_0, "Original Regular sampling");
        LOG(INFO) << "Original Regular sampling compression ratio = "
                  << 100.0 * (double) rlz_store_0.size_in_bytes() / (double) rlz_store_0.text_size;
       
        auto rlz_store_1 = rlz_type_zzz_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        compare_indexes(col,rlz_store_1, "Regular sampling");
        LOG(INFO) << "Regular sampling compression ratio = "
                  << 100.0 * (double) rlz_store_1.size_in_bytes() / (double) rlz_store_1.text_size;



        auto rlz_store_2 = rlz_type_zzz_greedy_sp_local_tb_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_2, "Local_tb_rand");
        LOG(INFO) << "Local_tb_rand compression ratio = "
                  << 100.0 * (double) rlz_store_2.size_in_bytes() / (double) rlz_store_2.text_size;


        auto rlz_store_3 = rlz_type_zzz_greedy_sp_local_cms_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_3, "Local_cms_rand");
    	LOG(INFO) << "Local_cms_rand compression ratio = "
                  << 100.0 * (double) rlz_store_3.size_in_bytes() / (double) rlz_store_3.text_size;


        auto rlz_store_4 = rlz_type_zzz_greedy_sp_local_tb_seq::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_4, "Local_tb_seq");
        LOG(INFO) << "Local_tb_seq compression ratio = "
                 << 100.0 * (double) rlz_store_4.size_in_bytes() / (double) rlz_store_4.text_size;


        auto rlz_store_5 = rlz_type_zzz_greedy_sp_local_cms_seq::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        compare_indexes(col,rlz_store_5, "Local_cms_seq");
    	LOG(INFO) << "Local_cms_seq compression ratio = "
                  << 100.0 * (double) rlz_store_5.size_in_bytes() / (double) rlz_store_5.text_size;
	
        

        //combining with pruning strategies
        //rem + regular sampling
        auto rlz_store_6 = rlz_type_zzz_greedy_sp_rem_regsamp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             //.set_dict_size(dict_size_in_bytes*4)
                             .set_dict_size(dict_size_in_bytes*2)
			     .set_pruned_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        compare_indexes(col,rlz_store_6, "REM + Regular sampling");
        LOG(INFO) << "REM + Regular sampling compression ratio = "
                  << 100.0 * (double) rlz_store_6.size_in_bytes() / (double) rlz_store_6.text_size;

        //rem + local_tb
        auto rlz_store_7 = rlz_type_zzz_greedy_sp_rem_local_tb::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             //.set_dict_size(dict_size_in_bytes*4)
                             .set_dict_size(dict_size_in_bytes*2)
			     .set_pruned_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_7, "REM + Local_tb_rand");
        LOG(INFO) << "REM + Local_tb_rand compression ratio = "
                  << 100.0 * (double) rlz_store_7.size_in_bytes() / (double) rlz_store_7.text_size;

        //rem + local_cms
        auto rlz_store_8 = rlz_type_zzz_greedy_sp_rem_local_cms::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                            // .set_dict_size(dict_size_in_bytes*4)
			     .set_dict_size(dict_size_in_bytes*2)
                             .set_pruned_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_8, "REM + Local_cms_rand");
        LOG(INFO) << "REM + Local_cms_rand compression ratio = "
                  << 100.0 * (double) rlz_store_8.size_in_bytes() / (double) rlz_store_8.text_size;

        //care + regular sampling
        auto rlz_store_9 = rlz_type_zzz_greedy_sp_care_regsamp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             //.set_dict_size(dict_size_in_bytes*4)
                             .set_dict_size(dict_size_in_bytes*2)
			     .set_pruned_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        compare_indexes(col,rlz_store_9, "CARE + Regular sampling");
        LOG(INFO) << "CARE + Regular sampling compression ratio = "
                  << 100.0 * (double) rlz_store_9.size_in_bytes() / (double) rlz_store_9.text_size;

        //care + local_tb
        auto rlz_store_10 = rlz_type_zzz_greedy_sp_care_local_tb::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             //.set_dict_size(dict_size_in_bytes*4)
                             .set_dict_size(dict_size_in_bytes*2)
			     .set_pruned_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_10, "CARE + Local_tb_rand");
        LOG(INFO) << "CARE + Local_tb_rand compression ratio = "
                  << 100.0 * (double) rlz_store_10.size_in_bytes() / (double) rlz_store_10.text_size;

        //care + local_cms
        auto rlz_store_11 = rlz_type_zzz_greedy_sp_care_local_cms::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             //.set_dict_size(dict_size_in_bytes*4)
                             .set_dict_size(dict_size_in_bytes*2)
			     .set_pruned_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_11, "CARE + Local_cms_rand");
        LOG(INFO) << "CARE + Local_cms_rand compression ratio = "
                  << 100.0 * (double) rlz_store_11.size_in_bytes() / (double) rlz_store_11.text_size;
 */
        auto rlz_store_12 = rlz_type_zzz_greedy_sp_local_norms_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_12, "Local_norms_rand");
        LOG(INFO) << "Local_norms_rand compression ratio = "
                  << 100.0 * (double) rlz_store_12.size_in_bytes() / (double) rlz_store_12.text_size;
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

    /* create rlz indices */
    create_indexes<32*1024*1024>(col,args);
//    create_indexes<64*1024*1024>(col,args);
//    create_indexes<512*1024*1024>(col,args);
//    create_indexes<1024*1024*1024>(col,args);
//    create_indexes<16*1024*1024>(col,args);
//    create_indexes<256*1024*1024>(col,args);

    return EXIT_SUCCESS;
}
