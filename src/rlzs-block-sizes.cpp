#include "utils.hpp"
#include "collection.hpp"
#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

typedef struct cmdargs {
    std::string collection_dir;
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
    while ((op = getopt(argc, (char* const*)argv, "c:f")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
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
    setup_logger(argc, argv, false);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    cmdargs_t args = parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* create rlz index */
    {
        const uint32_t dictionary_mem_budget_mb = 10;
        const uint32_t factorization_block_size = 1024*32;
        using dict_creation_strategy = dict_random_sample_budget<dictionary_mem_budget_mb, 1024>;
        using rlz_type = rlz_store_static<dict_creation_strategy,
                                          default_dict_pruning_strategy,
                                          default_dict_index_type,
                                          factorization_block_size,
                                          default_factor_selection_strategy,
                                          factor_coder_blocked<coder::u32, coder::vbyte>,
                                          default_block_map>;
        auto rlz_store = rlz_type::builder{}
                             .load(col);

        size_t prev = 0;
        for(size_t i=1;i<rlz_store.block_map.num_blocks();i++) {
        	auto offset = rlz_store.block_map.block_offset(i);
        	auto size_in_bits = offset - prev;
        	std::cout << i-1 << ";" << size_in_bits << "\n";
        }
        std::cout << rlz_store.block_map.num_blocks()-1 << ";"
        	<< rlz_store.text_size - rlz_store.block_map.block_offset(rlz_store.block_map.num_blocks()-1)
        	<< "\n";

    }

    return EXIT_SUCCESS;
}
