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
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        // using rlz_type_new = rlz_store_static<
        //                                  dict_random_uniform_hash<16>,
        //                                  dict_prune_none,
        //                                  dict_index_csa<csa_type>,
        //                                  factorization_blocksize,
        //                                  factor_select_first,
        //                                  fcoder_type,
        //                                  block_map_uncompressed>;
        // auto rlz_store_new = rlz_type_new::builder{}
        //                      .set_rebuild(args.rebuild)
        //                      .set_threads(args.threads)
        //                      .set_dict_size(args.dict_size_in_bytes)
        //                      .build_or_load(col);


        sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
        const uint32_t estimator_block_size = 16;
        using sketch_type = count_min_sketch<std::ratio<1, 3000000>,std::ratio<1, 10>>;
        chunk_freq_estimator_topk<estimator_block_size,500000,sketch_type> cfe_topk;

        for(size_t i=0;i<text.size();i++) {
            auto sym = text[i];
            cfe_topk.update(sym);
        }
        auto topk_blocks = cfe_topk.topk();
        auto threshold = topk_blocks.back().first;
        LOG(INFO) << "\t" << "Threshold freq = " << threshold;

        dict_index_csa<csa_type> idx(col,false);
        rabin_karp_hasher<estimator_block_size> rk;
        std::unordered_set<uint64_t> dict_hashes_used;
        auto not_found = dict_hashes_used.end();
        for(size_t i=0;i<estimator_block_size-1;i++) rk.update(text[i]); // init RKH
        LOG(INFO) << "pos;freq;factors;blocksize";
        for(size_t i=estimator_block_size-1;i<text.size();i++) {
            auto sym = text[i];
            auto hash = rk.update(sym);
            auto est = cfe_topk.estimate(hash);
            auto itr = dict_hashes_used.find(hash);
            if(est >= threshold && itr == not_found ) {
                auto beg = text.begin()+i-estimator_block_size;
                auto end = beg + estimator_block_size;
                auto factor_itr = idx.factorize(beg, end);
                uint64_t num_factors = 0;
                while (!factor_itr.finished()) {
                    num_factors++;
                    ++factor_itr;
                }
                dict_hashes_used.insert(hash);
                LOG(INFO) << i-estimator_block_size << ";" << est << ";" << num_factors << ";" << estimator_block_size;
            }

        }

        
    }

    return EXIT_SUCCESS;
}
