
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

enum class data_format {
    invalid,
    raw,
    trec,
    warc
};

typedef struct cmdargs {
    std::string collection_dir;
    std::string input_dir;
    std::string input_file;
    uint32_t max_num_files;
    data_format format;
} cmdargs_t;

void print_usage(const char* program)
{
    fprintf(stdout, "%s -c <collection directory> \n", program);
    fprintf(stdout, "where\n");
    fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
    fprintf(stdout, "  -i <input file>       : the raw input file.\n");
    fprintf(stdout, "  -t <input directory>  : the trec input dir.\n");
    fprintf(stdout, "  -w <input directory>  : the warc input dir.\n");
    fprintf(stdout, "  -n <max num files>    : maximum number of files to parse.\n");
};

cmdargs_t
parse_args(int argc, const char* argv[])
{
    cmdargs_t args;
    int op;
    args.collection_dir = "";
    args.input_dir = "";
    args.input_file = "";
    args.format = data_format::invalid;
    args.max_num_files = 99999999;
    while ((op = getopt(argc, (char* const*)argv, "c:t:w:n:i:")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
            break;
        case 'i':
            args.input_file = optarg;
            args.format = data_format::raw;
            break;
        case 't':
            args.input_dir = optarg;
            args.format = data_format::trec;
            break;
        case 'w':
            args.input_dir = optarg;
            args.format = data_format::warc;
            break;
        case 'n':
            args.max_num_files = (uint32_t)std::atoi(optarg);
            break;
        }
    }
    if (args.collection_dir == "" || args.format == data_format::invalid) {
        std::cerr << "Missing command line parameters.\n";
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    return args;
}

void
print_progress(double percent)
{
    uint32_t bar_width = 70;
    std::cout << "[";
    uint32_t pos = percent *bar_width;
    for(uint32_t i=0;i<bar_width;i++) {
        if(i<pos) std::cout << "=";
        else if(i==pos) std::cout << ">";
        else std::cout << " ";
    }
    std::cout << "] " << uint32_t(percent*100) << " %\r";
    std::cout.flush();
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    cmdargs_t args = parse_args(argc, argv);

    utils::create_directory(args.collection_dir);
    std::string output_file = args.collection_dir + "/" + KEY_PREFIX + KEY_TEXT;

    {
        //auto out = sdsl::write_out_buffer<8>::create(output_file);
        sdsl::int_vector_buffer<8> out(output_file,std::ios::out,128*1024*1024);
        if (args.format == data_format::raw) {
            if (!utils::file_exists(args.input_file)) {
                LOG(FATAL) << "Input file " << args.input_file << " does not exist.";
            }
            LOG(INFO) << "Processing " << args.input_file;
            sdsl::int_vector_buffer<8> input(args.input_file,std::ios::in,128*1024*1024,8,true);
            auto itr = input.begin();
            auto end = input.end();
            auto replaced_zeros = 0;
            auto replaced_ones = 0;
            size_t processed = 0;
            size_t total = std::distance(itr,end);
            size_t one_percent = total * 0.01;
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
                ++processed;
                if(processed % one_percent == 0) {
                    print_progress(double(processed)/double(total));
                }
            }
            std::cout << "\n";
            LOG(INFO) << "Replaced zeros = " << replaced_zeros;
            LOG(INFO) << "Replaced ones = " << replaced_ones;
        }
        else {
        }
        LOG(INFO) << "Copied " << out.size() << " bytes.";
    }
    {
        LOG(INFO) << "Writing document order file.";
        std::string docorder_file = args.collection_dir + "/" + KEY_PREFIX + KEY_DOCORDER;
        std::ofstream dof(docorder_file);
        sdsl::read_only_mapper<8> output(output_file);
        auto beg = output.begin();
        auto itr = output.begin();
        auto end = output.end();
        const std::string docno_start("<DOCNO>");
        const std::string docno_end("</DOCNO>");
        while (itr != end) {
            itr = std::search(itr, end, docno_start.begin(), docno_start.end());
            auto pos = std::distance(beg, itr);
            std::string cur_docno;
            if (itr != end) {
                /* found response */
                std::advance(itr, docno_start.size());
                auto resp_end = std::search(itr, end, docno_end.begin(), docno_end.end());
                while (itr != resp_end && itr != end) {
                    cur_docno += *itr;
                    ++itr;
                }
                dof << cur_docno << " " << pos << "\n";
            }
        }
    }

    return 0;
}
