
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"

#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP

typedef struct cmdargs {
    std::string collection_dir;
    std::string input_file;
} cmdargs_t;

void
print_usage(const char* program)
{
    fprintf(stdout, "%s -c <collection directory> \n", program);
    fprintf(stdout, "where\n");
    fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
    fprintf(stdout, "  -i <input file>  : the input file.\n");
};

cmdargs_t
parse_args(int argc, const char* argv[])
{
    cmdargs_t args;
    int op;
    args.collection_dir = "";
    args.input_file = "";
    while ((op = getopt(argc, (char* const*)argv, "c:i:")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
            break;
        case 'i':
            args.input_file = optarg;
            break;
        }
    }
    if (args.collection_dir == "" || args.input_file == "") {
        std::cerr << "Missing command line parameters.\n";
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    return args;
}

int main(int argc, const char* argv[])
{
    START_EASYLOGGINGPP(argc, argv);
    el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Format, "%datetime : %msg");

    cmdargs_t args = parse_args(argc, argv);

    if (!utils::file_exists(args.input_file)) {
        LOG(FATAL) << "Input file " << args.input_file << " does not exist.";
    }

    utils::create_directory(args.collection_dir);
    std::string output_file = args.collection_dir + "/" + KEY_PREFIX + KEY_TEXT;

    LOG(INFO) << "Copy " << args.input_file << " to " << output_file;

    auto out = sdsl::write_out_buffer<8>::create(output_file);
    sdsl::read_only_mapper<8> input(args.input_file, true);

    auto itr = input.begin();
    auto end = input.end();
    auto replaced_zeros = 0;
    auto replaced_ones = 0;
    while (itr != end) {
        auto sym = *itr;
        if (sym == 0) {
            sym = 0xFE;
            replaced_zeros++;
        }
        if (sym == 1) {
            replaced_ones++;
            sym = 0xFF;
        }
        out.push_back(sym);
        ++itr;
    }
    LOG(INFO) << "Replaced zeros = " << replaced_zeros;
    LOG(INFO) << "Replaced ones = " << replaced_ones;
    LOG(INFO) << "Copied " << out.size() << " bytes.";

    return 0;
}
