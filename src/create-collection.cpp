
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/filesystem.hpp> 
#include <boost/regex.hpp> 
#include <boost/range/iterator_range.hpp>

namespace io = boost::iostreams;
namespace fs = boost::filesystem;

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

void
print_usage(const char* program)
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
            args.max_num_files = (uint32_t) std::atoi(optarg);
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
parse_gz_file(sdsl::int_vector_mapper<8>& out,std::string file_name,data_format format)
{
    LOG(INFO) << "read file " << file_name;
    io::filtering_istream in;
    in.push(io::gzip_decompressor());
    in.push(io::file_source(file_name));
    std::noskipws(in);
    std::vector<uint8_t> buf;
    std::copy(std::istream_iterator<char>(in),std::istream_iterator<char>(),std::back_inserter(buf));

    uint64_t docs_found = 0;
    uint64_t written_bytes = 0;
    auto itr = buf.begin();
    auto end = buf.end();
    if(format == data_format::warc) {
        const std::string respone_start("WARC-Type: response");
        const std::string respone_end("WARC/1.0");
        while(itr != end) {
            itr = std::search(itr,end,respone_start.begin(),respone_start.end());
            if(itr != end) {
                /* found response */
                std::advance(itr,respone_start.size() + 1);
                auto resp_end = std::search(itr,end,respone_end.begin(),respone_end.end());
                while(itr != resp_end) {
                    if(*itr == 0) out.push_back(0xFE);
                    else out.push_back(*itr);
                    ++itr;
                    written_bytes++;
                }
                docs_found++;
            }
        }
    }
    if(format == data_format::trec) {
        const std::string doc_start("<DOC>");
        const std::string doc_end("</DOC>");
        while(itr != end) {
            itr = std::search(itr,end,doc_start.begin(),doc_start.end());
            if(itr != end) {
                /* found response */
                std::advance(itr,doc_start.size() + 1);
                auto resp_end = std::search(itr,end,doc_end.begin(),doc_end.end());
                while(itr != resp_end) {
                    if(*itr == 0) out.push_back(0xFE);
                    else out.push_back(*itr);
                    ++itr;
                    written_bytes++;
                }
                docs_found++;
            }
        }
    }
    LOG(INFO) << "bytes written = " << written_bytes;
    LOG(INFO) << "documents found = " << docs_found;
}

std::vector<std::string>
parse_dir(const std::string& dir) {
    std::vector<std::string> matches;
    const boost::regex my_filter( ".*\\.gz$" );

    fs::path gp(dir);
    if(fs::is_directory(gp)) {
        for(auto& entry : boost::make_iterator_range(fs::recursive_directory_iterator(gp), {})) {
            if(fs::is_regular_file(entry.status())) {
                if( !boost::regex_match( entry.path().string() , my_filter ) ) continue;
                matches.push_back(entry.path().string());
            }
        }
    }
    LOG(INFO) << "Found " << matches.size() << " files";
    return matches;
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    cmdargs_t args = parse_args(argc, argv);

    utils::create_directory(args.collection_dir);
    std::string output_file = args.collection_dir + "/" + KEY_PREFIX + KEY_TEXT;

    LOG(INFO) << "Parse " << args.input_dir << " to retrieve file names ";

    auto out = sdsl::write_out_buffer<8>::create(output_file);
    if(args.format == data_format::raw) {
        if (!utils::file_exists(args.input_file)) {
            LOG(FATAL) << "Input file " << args.input_file << " does not exist.";
        }
        sdsl::read_only_mapper<8> input(args.input_file,true);
        auto itr = input.begin();
        auto end = input.end();
        auto replaced_zeros = 0;
        auto replaced_ones = 0;
        while(itr != end) {
            auto sym = *itr;
            if(sym == 0) {
                sym = 0xFE;
                replaced_zeros++;
            }
            if(sym == 1) {
                replaced_ones++;
                sym = 0xFF;
            }
            out.push_back(sym);
            ++itr;
        }
        LOG(INFO) << "Replaced zeros = " << replaced_zeros;
        LOG(INFO) << "Replaced ones = " << replaced_ones;
    } else {
        if (!utils::directory_exists(args.input_dir)) {
            LOG(FATAL) << "Input directory " << args.input_file << " does not exist.";
        }
        /* (1) read file list */
        auto input_files = parse_dir(args.input_dir);
        /* (2) process files */
        for(size_t i=0;i<input_files.size();i++) {
            parse_gz_file(out,input_files[i],args.format);
            if(i+1 >= args.max_num_files) {
                LOG(INFO) << "Only processing " << i+1 << " files. STOP";
                break;
            }
        }
    }
    LOG(INFO) << "Copied " << out.size() << " bytes.";

    return 0;
}
