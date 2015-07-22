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
    const uint32_t block_size = 32768;
    const uint32_t prime_size = 32768;
    {
        auto lz_store = lz_store_static<coder::zlib<9>,block_size>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(0)
                             .build_or_load(col);

        if(args.verify) verify_index(col, lz_store);
    }
    {
        auto lz_store = lz_store_static<coder::zlib<9>,
                                    block_size,
                                    prime_size,
                                    dict_uniform_sample_budget<1024>
                                    >::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, lz_store);
    }



    return EXIT_SUCCESS;
}