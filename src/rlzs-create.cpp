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
    const uint32_t cikm_factorization_blocksize = 32768;
    {
        auto lz_store = lz_store_static<coder::zlib<9>,cikm_factorization_blocksize>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        if(args.verify) verify_index(col, lz_store);
    }

    {
        const uint32_t cikm_sample_block_size = 1024;
        using cikm_csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
        using rlz_type_uuv_greedy_sp = rlz_store_static<dict_uniform_sample_budget<cikm_sample_block_size>,
                                     dict_prune_none,
                                     dict_index_csa<cikm_csa_type>,
                                     cikm_factorization_blocksize,
                                     factor_select_first,
                                     factor_coder_blocked<4,coder::aligned_fixed<uint8_t>,coder::aligned_fixed<uint32_t>,coder::vbyte>,
                                     block_map_uncompressed>;
        auto rlz_store = rlz_type_uuv_greedy_sp::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        
        if(args.verify) verify_index(col, rlz_store);
    }



    return EXIT_SUCCESS;
}