#include "utils.hpp"
#include "collection.hpp"
#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

typedef struct cmdargs {
    std::string collection_dir;
    bool factor_decoding;
} cmdargs_t;

void
print_usage(const char* program)
{
    fprintf(stdout, "%s -c <collection directory> \n", program);
    fprintf(stdout, "where\n");
    fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
};

cmdargs_t
parse_args(int argc, const char* argv[])
{
    cmdargs_t args;
    int op;
    args.collection_dir = "";
    args.factor_decoding = false;
    while ((op = getopt(argc, (char* const*)argv, "c:f")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
            break;
        case 'f':
            args.factor_decoding = true;
            break;
        }
    }
    if (args.collection_dir == "") {
        std::cerr << "Missing command line parameters.\n";
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    return args;
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    cmdargs_t args = parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* load rlz index and benchmark */
    {
        using dict_strat = dict_random_sample_budget<100, 1024>;
        using dict_prune_strat = dict_prune_none;
        using factor_select_strat = factor_select_first;
        using factor_coder_strat = factor_coder_blocked<coder::u32, coder::vbyte>;
        using dict_index_type = dict_index_salcp; //default_dict_index_type
        using rlz_type = rlz_store_static<dict_strat,
                                          dict_prune_strat,
                                          dict_index_type,
                                          16384,
                                          factor_select_strat,
                                          factor_coder_strat,
                                          block_map_uncompressed>;

        /* load index */
        try {
            auto rlz_store = rlz_type::builder{}
                                 .load(col);

            /* measure factor decoding speed */
            if(args.factor_decoding) {
                LOG(INFO) << "Measure factor decoding speed";
                auto start = hrclock::now();
                auto itr = rlz_store.factors_begin();
                auto end = rlz_store.factors_end();

                size_t offset_checksum = 0;
                size_t len_checksum = 0;
                size_t num_factors = 0;
                while (itr != end) {
                    const auto& f = *itr;
                    offset_checksum += f.offset;
                    len_checksum += f.len;
                    num_factors++;
                    ++itr;
                }
                auto stop = hrclock::now();
                if (offset_checksum == 0 || len_checksum == 0) {
                    LOG(ERROR) << "factor decoding speed checksum error";
                }
                auto fact_seconds = duration_cast<milliseconds>(stop - start).count() / 1000.0;
                LOG(INFO) << "num_factors = " << num_factors;
                LOG(INFO) << "total time = " << fact_seconds << " sec";
                LOG(INFO) << "factors per sec = " << num_factors / fact_seconds;
                auto factors_mb = rlz_store.factor_text.size() / (double)(8*1024*1024); // bits to mb
                LOG(INFO) << "decoding speed = " << factors_mb / fact_seconds << " MB/s";
            } else { /* measure text decoding speed */
                LOG(INFO) << "Measure text decoding speed";
                auto start = hrclock::now();
                auto itr = rlz_store.begin();
                auto end = rlz_store.end();

                size_t checksum = 0;
                size_t num_syms = 0;
                auto n = rlz_store.size();
                for(size_t i=0;i<n;i++) {
                    checksum += *itr;
                    num_syms++;
                    ++itr;
                }
                auto stop = hrclock::now();
                if (checksum == 0) {
                    LOG(ERROR) << "text decoding speed checksum error";
                }
                auto text_seconds = duration_cast<milliseconds>(stop - start).count() / 1000.0;
                LOG(INFO) << "num syms = " << num_syms;
                LOG(INFO) << "total time = " << text_seconds << " sec";
                auto text_size_mb = num_syms / (1024 * 1024);
                LOG(INFO) << "decoding speed = " << text_size_mb / text_seconds << " MB/s";
                auto num_blocks = num_syms / rlz_store.factorization_block_size;
                LOG(INFO) << "num blocks = " << num_blocks;
                LOG(INFO) << "blocks per sec = " << num_blocks / text_seconds;
            }
        }
        catch (const std::runtime_error& e) {
            LOG(ERROR) << e.what();
            exit(EXIT_FAILURE);
        }
    }

    return EXIT_SUCCESS;
}
