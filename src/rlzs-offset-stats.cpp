#include "utils.hpp"
#include "collection.hpp"
#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv, false);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    cmdargs_t args = utils::parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* create rlz index */
    {
        const uint32_t dictionary_mem_budget_mb = 32;
        const uint32_t factorization_block_size = DEFAULT_FACTORIZATION_BLOCK_SIZE;
        using dict_creation_strategy = dict_random_sample_budget<dictionary_mem_budget_mb, DEFAULT_DICTIONARY_BLOCK_SIZE>;
        using rlz_type = rlz_store_static<dict_creation_strategy,
                                          default_dict_pruning_strategy,
                                          default_dict_index_type,
                                          factorization_block_size,
                                          default_factor_selection_strategy,
                                          default_factor_encoder,
                                          default_block_map>;
        auto rlz_store = rlz_type::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);

        std::vector<uint32_t> offset_cnts(rlz_store.dict.size());
        std::vector<uint32_t> offset_lens(rlz_store.dict.size());
        auto fitr = rlz_store.factors_begin();
        auto fend = rlz_store.factors_end();
        while (fitr != fend) {
            const auto& f = *fitr;
            if (f.len != 0) {
                for (size_t i = 0; i < f.len; i++) {
                    offset_cnts[f.offset + i]++;
                    offset_lens[f.offset + i] += f.len;
                }
            }
            ++fitr;
        }

        std::cout << "offset;freq;clen;avglen\n";
        for (size_t i = 0; i < offset_cnts.size(); i++) {
            std::cout << i << ";" << offset_cnts[i] << ";" << offset_lens[i] << ";"
                      << (double)offset_lens[i] / (double)offset_cnts[i] << "\n";
        }
    }

    return EXIT_SUCCESS;
}
