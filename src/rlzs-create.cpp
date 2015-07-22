#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template<size_t N, size_t P = 0>
constexpr typename std::enable_if<(N <= 1), size_t>::type CLog2()
{
   return P;
}

template<size_t N, size_t P = 0>
constexpr typename std::enable_if<!(N <= 1), size_t>::type CLog2()
{
   return CLog2<N / 2, P + 1>();
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
    const uint32_t block_size = 32768;
    const uint32_t dict_size_in_bytes_log2 = CLog2<64*1024*1024>();
    using csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
    /* RLZ-UV LOG(D)bit offsets */
    using rlz_type_uv_greedy_sp = rlz_store_static<dict_uniform_sample_budget<1024>,
                                 dict_prune_none,
                                 dict_index_csa<csa_type>,
                                 block_size,
                                 factor_select_first,
                                 factor_coder_blocked_twostream<1,coder::fixed<dict_size_in_bytes_log2>,coder::vbyte>,
                                 block_map_uncompressed>;
    auto rlz_store = typename rlz_type_uv_greedy_sp::builder{}
                         .set_rebuild(args.rebuild)
                         .set_threads(args.threads)
                         .set_dict_size(64*1024*1024)
                         .build_or_load(col);

    verify_index(col, rlz_store);

    return EXIT_SUCCESS;
}
