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
    using csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
    using rlz_type_zzz_greedy_sp = rlz_store_static<dict_uniform_sample_budget<1024>,
                                 dict_prune_rem<1024>,
                                 dict_index_csa<csa_type>,
                                 block_size,
                                 factor_select_first,
                                 factor_coder_blocked<1,coder::zlib<9>,coder::zlib<9>,coder::zlib<9>>,
                                 block_map_uncompressed>;
    auto rlz_store = typename rlz_type_zzz_greedy_sp::builder{}
                         .set_rebuild(args.rebuild)
                         .set_threads(args.threads)
                         .set_dict_size(64*1024*1024)
                         .set_pruned_dict_size(32*1024*1024)
                         .build_or_load(col);

    verify_index(col, rlz_store);

    return EXIT_SUCCESS;
}
