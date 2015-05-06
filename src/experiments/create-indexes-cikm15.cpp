#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "experiments/rlz_types_cikm15.hpp"

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
        auto rlz_store = rlz_type_zz_greedy_min::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        if (args.verify)
            verify_index(col, rlz_store);
    }
    {
        auto rlz_store = rlz_type_uv_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);
        if (args.verify)
            verify_index(col, rlz_store);

        auto rlz_store_new = rlz_type_zz_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .reencode(col,rlz_store);
    }
    {
        auto rlz_store = rlz_type_zz_lenmul2::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        if (args.verify)
            verify_index(col, rlz_store);
    }
    {
        auto rlz_store = rlz_type_zz_lenmul3::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        if (args.verify)
            verify_index(col, rlz_store);
    }

    return EXIT_SUCCESS;
}
