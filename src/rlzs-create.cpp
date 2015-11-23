#define ELPP_THREAD_SAFE

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
    const uint32_t factorization_blocksize = 64 * 1024;
    {
        auto rlz_store = typename rlz_type_u32v_greedy_sp<factorization_blocksize,false>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        verify_index(col, rlz_store);
        output_stats(rlz_store,"global");
    }
    {
        auto rlz_store = typename rlz_type_u32v_greedy_sp<factorization_blocksize,true>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        verify_index(col, rlz_store);
        output_stats(rlz_store,"global+local");
    }

    return EXIT_SUCCESS;
}
