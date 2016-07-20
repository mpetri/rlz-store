#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template<class t_idx>
double compute_archive_ratio(t_idx& idx)
{
    size_t bits_encoding = idx.factor_text.size();
    size_t bits_blockmap = idx.block_map.size_in_bytes() * 8;
    size_t bits_compressed_dict = 0;
    {
        const uint8_t* dict = (const uint8_t*) idx.dict.data();
        size_t dict_size = idx.dict.size();
        std::vector<uint8_t> dict_buf(dict_size*2);
        uint8_t* out_buf = dict_buf.data();
        uint64_t out_len = dict_buf.size();
        int cok = compress2(out_buf,&out_len,dict,dict_size,9);
        if(cok != Z_OK) {
            if(cok == Z_MEM_ERROR) LOG(FATAL) << "error compressing dictionary: Z_MEM_ERROR";
            if(cok == Z_BUF_ERROR) LOG(FATAL) << "error compressing dictionary: Z_BUF_ERROR";
            if(cok == Z_STREAM_ERROR) LOG(FATAL) << "error compressing dictionary: Z_STREAM_ERROR";
            LOG(FATAL) << "error compressing ditionary: UNKNOWN ERROR ("<<cok<<")";
        }
        bits_compressed_dict = out_len * 8;
    }
    size_t encoding_bits_total = bits_encoding + bits_compressed_dict + bits_blockmap;
    size_t text_size_bits = idx.size() * 8;
    
    double archive_ratio = 100.0 * double(encoding_bits_total) / double(text_size_bits);
    return archive_ratio;
}

template<class t_idx>
double compute_archive_ratio_lz(t_idx& idx)
{
    size_t text_size_bits = idx.size() * 8;

    size_t bits_encoding = idx.compressed_text.size();
    size_t bits_blockmap = idx.block_map.size_in_bytes() * 8;
    size_t encoding_bits_total = bits_encoding + bits_blockmap;
    double archive_ratio = 100.0 * double(encoding_bits_total) / double(text_size_bits);
    return archive_ratio;
}

template <uint32_t dict_size_in_bytes>
void create_indexes(collection& col, utils::cmdargs_t& args,std::string col_name)
{    
    const uint32_t factorization_blocksize = 64 * 1024;
    {
        auto rlz_store = rlz_store_static<dict_local_coverage_norms<1024,16,512,std::ratio<1,2>,RAND>,
                    dict_prune_none,
                    dict_index_sa,
                    factorization_blocksize,
                    false,
                    factor_select_first,
                    factor_coder_blocked<3, coder::zlib<9>, coder::zlib<9>, coder::zlib<9> >,
                    block_map_uncompressed>::builder{}
                    .set_rebuild(args.rebuild)
                    .set_threads(args.threads)
                    .set_dict_size(dict_size_in_bytes)
                    .build_or_load(col);
                    
        uint32_t dict_size_mib = dict_size_in_bytes / (1024*1024);
            
        //verify_index(col, rlz_store);
        double archive_ratio = compute_archive_ratio(rlz_store);

        LOG(INFO) << col_name << ";RLZ-WWW-" << dict_size_mib << ";" << archive_ratio;
    }

    {
        auto lz_store = typename lz_store_static<coder::zlib<9>, factorization_blocksize>::builder{}
                            .set_rebuild(args.rebuild)
                            .set_threads(args.threads)
                            .build_or_load(col);
        //verify_index(col, lz_store);
        double archive_ratio = compute_archive_ratio_lz(lz_store);
        LOG(INFO) << col_name << ";ZLIB-9;" << archive_ratio;
    }
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    auto args = utils::parse_args(argc, argv);
    std::string base_path = args.collection_dir;
    std::string col_name = sdsl::util::basename(base_path);
    std::string col_dir = base_path;

    /* parse the collection */
    LOG(INFO) << "col name = " << col_name;
    LOG(INFO) << "col dir = " << col_dir;
    collection col(col_dir);
    create_indexes<16 * 1024 * 1024>(col, args,col_name);

    return EXIT_SUCCESS;
}
