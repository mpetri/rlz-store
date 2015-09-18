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
        auto rlz_store_1 = rlz_type_zzz_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        compare_indexes(col,rlz_store_1, "Regular sampling");
        // verify_index(col, rlz_store);


        auto rlz_store_2 = rlz_type_zzz_greedy_sp_local_tb_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_2, "Local_tb_rand");
        // verify_index(col, rlz_store);


        auto rlz_store_3 = rlz_type_zzz_greedy_sp_local_cms_rand::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_3, "Local_cms_rand");
        // verify_index(col, rlz_store);


        auto rlz_store_4 = rlz_type_zzz_greedy_sp_local_tb_seq::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);
        compare_indexes(col,rlz_store_4, "Local_tb_seq");
        // verify_index(col, rlz_store);


        auto rlz_store_5 = rlz_type_zzz_greedy_sp_local_cms_seq::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        compare_indexes(col,rlz_store_5, "Local_cms_seq");
        // verify_index(col, rlz_store);
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
    create_indexes<64*1024*1024>(col,args);

    return EXIT_SUCCESS;
}