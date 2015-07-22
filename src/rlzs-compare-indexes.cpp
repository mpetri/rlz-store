#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

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
    {
    	/* create the default dict */
		const uint32_t factorization_blocksize = 32768;
		const uint32_t sample_block_size = 1024;
		using csa_type = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 1, 4096>;
		using fcoder_type = factor_coder_blocked<1, coder::fixed<32>, coder::aligned_fixed<uint32_t>, coder::vbyte>;
		using rlz_type_uv_greedy_sp = rlz_store_static<
										 dict_uniform_sample_budget<sample_block_size>,
			                             dict_prune_none,
			                             dict_index_csa<csa_type>,
			                             factorization_blocksize,
			                             factor_select_first,
			                             fcoder_type,
			                             block_map_uncompressed>;
        auto rlz_store = rlz_type_uv_greedy_sp::builder{}
                             .set_rebuild(false)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        using rlz_type_new = rlz_store_static<
                                         dict_sketch_cover_onepass<1024,8,100>,
                                         // dict_local_coverage_disjoint<sample_block_size, 16>,
                                         // dict_local_coverage_nobias_memory<sample_block_size, 16>,
                                         //dict_local_coverage_nobias_sorted<sample_block_size, 16>,
                                         // dict_local_coverage_rolling<sample_block_size, 8>,
                                         dict_prune_none,
                                         dict_index_csa<csa_type>,
                                         factorization_blocksize,
                                         factor_select_first,
                                         fcoder_type,
                                         block_map_uncompressed>;
        auto rlz_store_new = rlz_type_new::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);


        compare_indexes(col,rlz_store,rlz_store_new);
        
    }

    return EXIT_SUCCESS;
}
