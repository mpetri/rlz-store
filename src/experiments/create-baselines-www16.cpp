#define ELPP_THREAD_SAFE

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
        auto rlz_store = rlz_type_zzz_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(dict_size_in_bytes)
                             .build_or_load(col);

        verify_index(col, rlz_store);
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
    create_indexes<64*1024*1024>(col,args);
    create_indexes<256*1024*1024>(col,args);
    create_indexes<1024*1024*1024>(col,args);

    return EXIT_SUCCESS;
}