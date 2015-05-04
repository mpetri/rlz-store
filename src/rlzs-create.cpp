#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"

#include "indexes.hpp"

#include "logging.hpp"
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

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    cmdargs_t args = parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* create rlz index */
    { // use uncompressed sa for factorization
        using dict_strat = dict_random_sample_budget<100, 1024>;
        using dict_prune_strat = dict_prune_none;
        using factor_select_strat = factor_select_minimum; //factor_select_first;
        const int keep_length_multiple_of = 2;
        using factor_coder_strat = factor_coder_blocked<coder::u32, coder::length_multiplier<keep_length_multiple_of, coder::vbyte> >; // factor_coder<coder::u32, coder::vbyte>;
        using dict_index_type = dict_index_sa_length_selector<keep_length_multiple_of>; // dict_index_salcp; //default_dict_index_type
        using rlz_type = rlz_store_static<dict_strat,
                                          dict_prune_strat,
                                          dict_index_type,
                                          8192,
                                          factor_select_strat,
                                          factor_coder_strat,
                                          block_map_uncompressed>;
        auto rlz_store = rlz_type::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .build_or_load(col);

        if (args.verify)
            col.verify_index(rlz_store);
    }

    return EXIT_SUCCESS;
}
