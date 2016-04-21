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
        auto rlz_store = rlz_store_static<dict_uniform_sample_budget<default_dict_sample_block_size>,
                             dict_prune_none,
                             dict_index_sa,
                             factorization_blocksize,
                             false,
                             factor_select_first,
                             factor_coder_blocked<3, coder::zlib<9>, coder::zlib<9>, coder::zlib<9> >,
                             block_map_uncompressed>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(8 * 1024 * 1024)
                             .build_or_load(col);

        verify_index(col, rlz_store);
    }
    // {
    //     auto rlz_store = rlz_store_static<dict_uniform_sample_budget<default_dict_sample_block_size>,
    //                          dict_prune_none,
    //                          default_dict_index_type,
    //                          factorization_blocksize,
    //                          false,
    //                          factor_select_first,
    //                          factor_coder_blocked<3, coder::zlib<9>, coder::zlib<9>, coder::zlib<9> >,
    //                          block_map_uncompressed>::builder{}
    //                          .set_rebuild(args.rebuild)
    //                          .set_threads(args.threads)
    //                          .set_dict_size(8 * 1024 * 1024)
    //                          .build_or_load(col);

    //     verify_index(col, rlz_store);
    // }



    return EXIT_SUCCESS;
}
