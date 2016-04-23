#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template<class t_idx>
void compute_archive_ratio(collection& col, t_idx& idx,std::string index_name)
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
            LOG(FATAL) << "error compressing dictionary.";
        }
        bits_compressed_dict = out_len * 8;
    }
    size_t encoding_bits_total = bits_encoding + bits_compressed_dict + bits_blockmap;
    size_t text_size_bits = idx.size() * 8;
    
    double archive_ratio = 100.0 * double(encoding_bits_total) / double(text_size_bits);
    
    LOG(INFO) << index_name << " Encoding size = " << double(bits_encoding) / double(1024*1024*8) << " MiB";
    LOG(INFO) << index_name << " Blockmap size = " << double(bits_blockmap) / double(1024*1024*8) << " MiB";
    LOG(INFO) << index_name << " Compressed dict size = " << double(bits_compressed_dict) / double(1024*1024*8) << " MiB";
    LOG(INFO) << index_name << " Archive Ratio = " << archive_ratio << " %";
}

template <uint32_t dict_size_in_bytes>
void create_indexes(collection& col, utils::cmdargs_t& args)
{    /* create rlz index */
    const uint32_t factorization_blocksize = 64 * 1024;
    {
        auto rlz_store = rlz_store_static<dict_local_coverage_norms<1024,16,512,std::ratio<1,2>>,
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
                    
        uint32_t text_size_mib = rlz_store.size() / (1024*1024);
        uint32_t dict_size_mib = dict_size_in_bytes / (1024*1024);
        std::string index_name = "GOV2S-WWW-" + std::to_string(text_size_mib) + "-" + std::to_string(dict_size_mib);
                    
        verify_index(col, rlz_store);
        compute_archive_ratio(col,rlz_store,index_name);
    }
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    auto args = utils::parse_args(argc, argv);
    collection col(args.collection_dir);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;

    /* create rlz indices */
    create_indexes<1 * 1024 * 1024>(col, args);
    create_indexes<2 * 1024 * 1024>(col, args);
    create_indexes<4 * 1024 * 1024>(col, args);
    create_indexes<8 * 1024 * 1024>(col, args);
    create_indexes<16 * 1024 * 1024>(col, args);
    create_indexes<32 * 1024 * 1024>(col, args);
    create_indexes<64 * 1024 * 1024>(col, args);
    create_indexes<128 * 1024 * 1024>(col, args);
    create_indexes<256 * 1024 * 1024>(col, args);
    create_indexes<512 * 1024 * 1024>(col, args);

    return EXIT_SUCCESS;
}
