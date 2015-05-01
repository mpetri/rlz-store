#include "utils.hpp"
#include "collection.hpp"

#include "indexes.hpp"

INITIALIZE_EASYLOGGINGPP

typedef struct cmdargs {
    std::string collection_dir;
    bool rebuild;
    uint32_t threads;
    bool verify;
} cmdargs_t;

void
print_usage(const char* program)
{
    fprintf(stdout, "%s -c <collection directory> \n", program);
    fprintf(stdout, "where\n");
    fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
    fprintf(stdout, "  -d <debug output>          : increase the amount of logs shown.\n");
    fprintf(stdout, "  -t <threads>               : number of threads to use during factorization.\n");
    fprintf(stdout, "  -f <force rebuild>         : force rebuild of structures.\n");
    fprintf(stdout, "  -v <verify index>          : verify the factorization can be used to recover the text.\n");
};

cmdargs_t
parse_args(int argc, const char* argv[])
{
    cmdargs_t args;
    int op;
    args.collection_dir = "";
    args.rebuild = false;
    args.verify = false;
    args.threads = 1;
    while ((op = getopt(argc, (char* const*)argv, "c:fdvt:")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
            break;
        case 'f':
            args.rebuild = true;
            break;
        case 't':
            args.threads = std::stoul(optarg);
            break;
        case 'd':
            el::Loggers::setLoggingLevel(el::Level::Trace);
            break;
        case 'v':
            args.verify = true;
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

template <class t_idx>
void verify_index(collection& col, t_idx& rlz_store)
{
    LOG(INFO) << "Verify that factorization is correct.";
    sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
    auto num_blocks = text.size() / rlz_store.factorization_block_size;

    bool error = false;
    for (size_t i = 0; i < num_blocks; i++) {
        auto block_content = rlz_store.block(i);
        auto block_start = i * rlz_store.factorization_block_size;
        if (block_content.size() != rlz_store.factorization_block_size) {
            error = true;
            LOG_N_TIMES(100, ERROR) << "Error in block " << i
                                    << " block size = " << block_content.size()
                                    << " factorization_block_size = " << rlz_store.factorization_block_size;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), text.begin() + block_start);
        if (!eq) {
            error = true;
            LOG(ERROR) << "BLOCK " << i << " NOT EQUAL";
            for (size_t j = 0; j < rlz_store.factorization_block_size; j++) {
                if (text[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)text[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
            exit(EXIT_FAILURE);
        }
    }
    auto left = text.size() % rlz_store.factorization_block_size;
    if (left) {
        auto block_content = rlz_store.block(num_blocks);
        auto block_start = num_blocks * rlz_store.factorization_block_size;
        if (block_content.size() != left) {
            error = true;
            LOG_N_TIMES(100, ERROR) << "Error in  LAST block "
                                    << " block size = " << block_content.size()
                                    << " left  = " << left;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), text.begin() + block_start);
        if (!eq) {
            error = true;
            LOG(ERROR) << "LAST BLOCK IS NOT EQUAL";
            for (size_t j = 0; j < left; j++) {
                if (text[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)text[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
        }
    }
    if (!error) {
        LOG(INFO) << "SUCCESS! Text sucessfully recovered.";
    }
}

int main(int argc, const char* argv[])
{
    utils::setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    cmdargs_t args = parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* create rlz index */
    /*
    {
<<<<<<< HEAD

        auto rlz_store = rlz_type_standard::builder{}
                            .set_rebuild(args.rebuild)
                            .set_threads(args.threads)
                            .build_or_load(col);

        if(args.verify) verify_index(col,rlz_store);
    }*/
    { // use uncompressed sa for factorization
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
        auto rlz_store = rlz_type::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .build_or_load(col);

        if (args.verify)
            verify_index(col, rlz_store);
    }

    return EXIT_SUCCESS;
}
