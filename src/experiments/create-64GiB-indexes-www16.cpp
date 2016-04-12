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
	utils::flush_cache(); 
         auto rlz_store_1 = rlz_type_zzz_greedy_sp_4::builder{}
                              .set_rebuild(args.rebuild)
                              .set_threads(args.threads)
                              .set_dict_size(dict_size_in_bytes)
                              .build_or_load(col);
		


        auto rlz_store_3 = rlz_type_zzz_greedy_sp_local_cms_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_3, "Local_cms_rand");
    	LOG(INFO) << "Local_cms_rand compression ratio = "
                  << 100.0 * (double) rlz_store_3.size_in_bytes() / (double) rlz_store_3.text_size;



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
        
	utils::flush_cache();
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
 
	utils::flush_cache();

        auto rlz_store_12 = rlz_type_zzz_greedy_sp_local_half_norm_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_12, "Local_half_norm_rand");
        LOG(INFO) << "Local_half_norm_rand compression ratio = "
                  << 100.0 * (double) rlz_store_12.size_in_bytes() / (double) rlz_store_12.text_size;

	auto rlz_store_12 = rlz_type_zzz_greedy_sp_local_zero_norm_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_12, "Local_zero_norm_rand");
        LOG(INFO) << "Local_zero_norm_rand compression ratio = "
                  << 100.0 * (double) rlz_store_12.size_in_bytes() / (double) rlz_store_12.text_size;

        auto rlz_store_12 = rlz_type_zzz_greedy_sp_local_one_norm_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_12, "Local_one_norm_rand");
        LOG(INFO) << "Local_one_norm_rand compression ratio = "
                  << 100.0 * (double) rlz_store_12.size_in_bytes() / (double) rlz_store_12.text_size;

	auto rlz_store_13 = rlz_type_zzz_greedy_sp_local_onehalf_norm_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_13, "Local_onehalf_norm_rand");
        LOG(INFO) << "Local_onehalf_norm_rand compression ratio = "
                  << 100.0 * (double) rlz_store_13.size_in_bytes() / (double) rlz_store_13.text_size;
*/
	 //care + local_half_norm
    /*    auto rlz_store_11 = rlz_type_zzz_greedy_sp_care_local_half_norm::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             //.set_dict_size(dict_size_in_bytes*4)
                             .set_dict_size(dict_size_in_bytes*2)
                             .set_pruned_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_11, "CARE + Local_half_norm");
        LOG(INFO) << "CARE + Local_half_norm compression ratio = "
                  << 100.0 * (double) rlz_store_11.size_in_bytes() / (double) rlz_store_11.text_size;	

      const uint32_t factorization_blocksize = 64 * 1024;
        auto rlz_store_12 = rlz_type_zzz_greedy_sp_assembly<factorization_blocksize,false>::builder{}
            .set_rebuild(args.rebuild)
            .set_threads(args.threads)
            .set_dict_size(dict_size_in_bytes)
            .build_or_load(col);
        compare_indexes(col,rlz_store_12, "Assembly");
        LOG(INFO) << "Assembly compression ratio = "
                  << 100.0 * (double) rlz_store_12.size_in_bytes() / (double) rlz_store_12.text_size;
        
         const uint32_t factorization_blocksize = 64 * 1024;
        auto rlz_store_12 = rlz_type_zzz_greedy_sp_deduplicate<factorization_blocksize,false>::builder{}
            .set_rebuild(args.rebuild)
            .set_threads(args.threads)
            .set_dict_size(dict_size_in_bytes)
            .build_or_load(col);
        compare_indexes(col,rlz_store_12, "Deduplicate");
        LOG(INFO) << "Deduplicate compression ratio = "
                  << 100.0 * (double) rlz_store_12.size_in_bytes() / (double) rlz_store_12.text_size;
 */
        const uint32_t factorization_blocksize = 64 * 1024;
        // const uint32_t sampling_blocksize = 512;          
        auto rlz_store_0 = rlz_type_zzz_greedy_sp_rs<factorization_blocksize,false>::builder{}
                                 .set_rebuild(args.rebuild)
                                 .set_threads(args.threads)
                                 .set_dict_size(dict_size_in_bytes)
                                 .build_or_load(col);

        compare_indexes(col,rlz_store_0, "Dictionary construction via regular sampling");
        LOG(INFO) << "Dictionary Size in MB = " 
                  << dict_size_in_bytes/(1024*1024);
        LOG(INFO) << "Regular sampling compression ratio (include dic) = " 
                  << 100.0 * (double) rlz_store_0.size_in_bytes() / (double) rlz_store_0.text_size;    
        LOG(INFO) << "Regular sampling compression ratio (exclude dic) = "
                  << 100.0 * (double) (rlz_store_0.size_in_bytes() - dict_size_in_bytes) / (double) rlz_store_0.text_size;    

        // utils::flush_cache();
        auto rlz_store = rlz_type_zzz_greedy_sp_local_half_norm_rand<factorization_blocksize,false>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store, "Local_half_norm_rand");
        LOG(INFO) << "Local_half_norm_rand compression ratio (include dic) = "
                  << 100.0 * (double) rlz_store.size_in_bytes() / (double) rlz_store.text_size;
        LOG(INFO) << "Local_half_norm_rand compression ratio (exclude dic) = "
                  << 100.0 * (double) (rlz_store.size_in_bytes() - dict_size_in_bytes)/ (double) rlz_store.text_size;

        // const uint32_t factorization_blocksize = 64 * 1024;
        // const uint32_t sampling_blocksize = 1 * 1024;          
        // auto rlz_store_0 = rlz_type_zzz_greedy_sp_initial_localremplusplus<factorization_blocksize,sampling_blocksize,false>::builder{}
        //                          .set_rebuild(args.rebuild)
        //                          .set_threads(args.threads)
        //                          .set_dict_size(dict_size_in_bytes)
        //                          .build_or_load(col);

        // compare_indexes(col,rlz_store_0, "Initial dictionary construction via REM++");
        // LOG(INFO) << "Dictionary Size in MB = " 
        //           << dict_size_in_bytes/(1024*1024);
        // LOG(INFO) << "Initial REM++ compression ratio (include dic) = " 
        //           << 100.0 * (double) rlz_store_0.size_in_bytes() / (double) rlz_store_0.text_size;    
        // LOG(INFO) << "Initial REM++ compression ratio (exclude dic) = "
        //           << 100.0 * (double) (rlz_store_0.size_in_bytes() - dict_size_in_bytes) / (double) rlz_store_0.text_size;    

        //care + regular sampling
        // auto rlz_store_9 = rlz_type_zzz_greedy_sp_care_regsamp<factorization_blocksize,sampling_blocksize,false>::builder{}
        //                      .set_rebuild(args.rebuild)
        //                      .set_threads(args.threads)
        //                      //.set_dict_size(dict_size_in_bytes*4)
        //                      .set_dict_size(dict_size_in_bytes*2)
        //                      .set_pruned_dict_size(dict_size_in_bytes)
        //                      .build_or_load(col);

        // compare_indexes(col,rlz_store_9, "CARE + Regular sampling");
        // LOG(INFO) << "Dictionary Size in MB = " 
        //           << dict_size_in_bytes/(1024*1024);
        // LOG(INFO) << "CARE + Regular sampling compression ratio (include dic) = " 
        //           << 100.0 * (double) rlz_store_9.size_in_bytes() / (double) rlz_store_9.text_size;    
        // LOG(INFO) << "CARE + Regular sampling compression ratio (exclude dic) = "
        //           << 100.0 * (double) (rlz_store_9.size_in_bytes() - dict_size_in_bytes) / (double) rlz_store_9.text_size;    
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
     create_indexes<16*1024*1024>(col,args);
//    create_indexes<32*1024*1024>(col,args); 
//    create_indexes<64*1024*1024>(col,args);
   // create_indexes<128*1024*1024>(col,args);
   // create_indexes<1024*1024*1024>(col,args);
   // create_indexes<256*1024*1024>(col,args);
   // create_indexes<2048*1024*1024L>(col,args);
    return EXIT_SUCCESS;
}
