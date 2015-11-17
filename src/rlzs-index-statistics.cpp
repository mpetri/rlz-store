#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template <class t_idx>
void output_block_stats(t_idx& idx, std::string name)
{
    for (size_t block_id = 0; block_id < idx.block_map.num_blocks(); block_id++) {

        uint64_t block_size_bits = idx.factor_text.size() - idx.block_map.m_block_offsets[block_id];
        if(block_id+1 != idx.block_map.num_blocks()) block_size_bits = idx.block_map.m_block_offsets[block_id+1] - idx.block_map.m_block_offsets[block_id];
        uint64_t block_size_bytes = block_size_bits / 8;
        uint64_t num_factors_per_block = idx.block_map.m_block_factors[block_id];
        double block_cr_ratio = (double) block_size_bytes / (double) idx.encoding_block_size;

        // factor stats
        auto fdat = idx.block_factors(block_id);
        auto factor_data = fdat.second;
        auto block_size_data = fdat.first;
        uint64_t num_literals = 0;
        uint64_t num_non_literals = 0;
        uint64_t total_factor_len = 0;
        std::vector<uint64_t> dict_blocks;
        for(const auto& fd : factor_data) {
            if(fd.is_literal) {
                num_literals++;
            } else {
                num_non_literals++;
                total_factor_len += fd.len;
                auto dict_block = fd.offset / 1024;
                dict_blocks.push_back(dict_block);
            }
        }
        double avg_factor_len = 0;
        if(num_non_literals != 0) avg_factor_len = (double) total_factor_len / (double) num_non_literals;

        std::sort(dict_blocks.begin(),dict_blocks.end());
        auto end = std::unique(dict_blocks.begin(),dict_blocks.end());
        dict_blocks.erase(end,dict_blocks.end());
        std::adjacent_difference(dict_blocks.begin(), dict_blocks.end(), dict_blocks.begin());

        double block_id_clust = 0.0;
        for(const auto& db : dict_blocks) {
            if(db <= 1) block_id_clust += 1;
            else block_id_clust += (sdsl::bits::hi(db-1)+1);
        }

        uint64_t num_dblocks = (idx.dict.size()-2) / 1024;
        double in_segment_bits = sdsl::bits::hi(1023)+1;
        double regular_block_id_cost = (sdsl::bits::hi(num_dblocks)+1);
        double gapped_block_id_cost = sdsl::bits::hi(dict_blocks.size())+1;

        double regular_offset_cost = (regular_block_id_cost + in_segment_bits) * num_non_literals;
        double gapped_offset_cost = block_id_clust + (gapped_block_id_cost + in_segment_bits) * num_non_literals;

        double regular_offset_cost_per_fac = regular_offset_cost / num_non_literals;
        double gapped_offset_cost_per_fac = gapped_offset_cost / num_non_literals;

        std::cout << name << ";"
                  << block_id << ";"
                  << block_size_bytes << ";"
                  << block_size_data.literal_bytes << ";"
                  << block_size_data.length_bytes << ";"
                  << block_size_data.offset_bytes << ";"
                  << block_cr_ratio << ";"
                  << num_factors_per_block << ";"
                  << num_literals << ";"
                  << num_non_literals << ";"
                  << avg_factor_len << ";"
                  << total_factor_len << ";"
                  << dict_blocks.size() << ";"
                  << in_segment_bits << ";"
                  << regular_block_id_cost << ";"
                  << gapped_block_id_cost << ";"
                  << regular_offset_cost << ";"
                  << gapped_offset_cost << ";"
                  << regular_offset_cost_per_fac << ";"
                  << gapped_offset_cost_per_fac << "\n";
    }
}

template <class t_idx>
void sample_block_offset_dists(t_idx& idx)
{
    auto num_blocks = idx.block_map.num_blocks();
    std::vector<uint64_t> blocks(num_blocks);
    for(size_t i=0;i<num_blocks;i++) blocks[i] = i;
    std::mt19937 g(4711);
    std::shuffle(blocks.begin(),blocks.end(), g);
    blocks.resize(1000);
    for(auto bid : blocks) {
        auto fdat = idx.block_factors(bid);
        auto factor_data = fdat.second;
        for(const auto& fd : factor_data) {
            if(!fd.is_literal) {
                std::cout << bid << ";" << fd.offset << ";" << fd.len << "\n";
            }
        }
    }
}

template <class t_idx>
void sample_dict_block_offset_dists(t_idx& idx)
{
    auto dict_size = idx.dict.size();
    auto num_dict_blocks = dict_size /  1024;
    std::mt19937 g(4711);
    std::vector<uint64_t> dict_blocks(num_dict_blocks);
    for(size_t i=0;i<num_dict_blocks;i++) dict_blocks[i] = i;
    std::shuffle(dict_blocks.begin(),dict_blocks.end(), g);
    dict_blocks.resize(100);

    std::unordered_set<uint64_t> potential_offsets;
    for(size_t i=0;i<dict_blocks.size();i++) {
        auto dblock = dict_blocks[i];
        auto dstart = dblock * 1024;
        for(size_t j=0;j<1024;j++) {
            potential_offsets.insert(dstart+j);
        }
    }

    auto num_blocks = idx.block_map.num_blocks();
    size_t text_offset = 0;
    for(size_t block_id=0;block_id<num_blocks;block_id++) {
        auto fdat = idx.block_factors(block_id);
        auto factor_data = fdat.second;
        text_offset  = block_id* idx.encoding_block_size;
        for(const auto& fd : factor_data) {
            if(fd.is_literal) {
            } else {
                if(potential_offsets.count(fd.offset) != 0) {
                    auto dict_block = fd.offset / 1024;
                    std::cout << text_offset << ";" << fd.offset << ";" << fd.len << ";" << dict_block << "\n";
                }
            }
            text_offset += fd.len;
        }
    }
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    auto args = utils::parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    std::cout << "type" << ";"
              << "block_id" << ";"
              << "block_size_bytes" << ";"
              << "literal_bytes" << ";"
              << "length_bytes" << ";"
              << "offset_bytes" << ";"
              << "block_cr_ratio" << ";"
              << "num_factors_per_block" << ";"
              << "num_literals" << ";"
              << "num_non_literals" << ";"
              << "avg_factor_len" << ";"
              << "total_factor_len" << ";"
              << "dict_blocks_referenced" << ";"
              << "in_segment_bits" << ";"
              << "regular_block_id_cost" << ";"
              << "gapped_block_id_cost" << ";"
              << "regular_offset_cost" << ";"
              << "gapped_offset_cost" << ";"
              << "regular_offset_cost_per_fac" << ";"
              << "gapped_offset_cost_per_fac" << "\n";

    /* create rlz index */
    // {
    //     const uint32_t factorization_blocksize = 64 * 1024;
    //     auto rlz_store = typename rlz_type_u32v_greedy_sp<factorization_blocksize>::builder{}
    //                          .set_rebuild(args.rebuild)
    //                          .set_threads(args.threads)
    //                          .set_dict_size(args.dict_size_in_bytes)
    //                          .build_or_load(col);

    //     output_block_stats(rlz_store, "RLZ-U8U32V");
    // }

    // std::cout << "text_offset" << ";"
    //           << "offset" << ";"
    //           << "len" << ";"
    //           << "dict_block" << "\n";

    {
        const uint32_t factorization_blocksize = 64 * 1024;
        auto rlz_store = typename rlz_type_zzz_greedy_sp<factorization_blocksize>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col);
        output_block_stats(rlz_store, "RLZ-ZZZ");
    }

    return EXIT_SUCCESS;
}
