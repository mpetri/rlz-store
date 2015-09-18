#pragma once

#include <chrono>

#include "utils.hpp"
#include "factor_storage.hpp"

using namespace std::chrono;

template <class t_idx>
void benchmark_factor_decoding(const t_idx& idx)
{
    LOG(INFO) << "Measure factor decoding speed (" << idx.type() << ")";
    auto start = hrclock::now();
    auto itr = idx.factors_begin();
    auto end = idx.factors_end();

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
    auto factors_mb = idx.factor_text.size() / (double)(8 * 1024 * 1024); // bits to mb
    LOG(INFO) << "decoding speed = " << factors_mb / fact_seconds << " MB/s";
}

template <class t_idx>
void benchmark_text_decoding(const t_idx& idx)
{
    LOG(INFO) << "Measure text decoding speed (" << idx.type() << ")";
    auto start = hrclock::now();
    auto itr = idx.begin();
    auto end = idx.end();

    size_t checksum = 0;
    size_t num_syms = 0;
    auto n = idx.size();
    for (size_t i = 0; i < n; i++) {
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
    auto num_blocks = num_syms / t_idx::block_size;
    LOG(INFO) << "num blocks = " << num_blocks;
    LOG(INFO) << "blocks per sec = " << num_blocks / text_seconds;
    LOG(INFO)<< "text checksum = "<< checksum;


}

template <class t_idx>
void print_compressed_size(collection& col,t_idx& idx) 
{
    size_t compressed_size = 0;
    {
        auto factor_file_name = t_idx::factorization_strategy::factor_file_name(col);
        sdsl::read_only_mapper<8> factorfile(factor_file_name);
        compressed_size = factorfile.size();     
    }

    LOG(INFO)<<" compressed size = "
    << compressed_size << " in bytes "
    << (double)compressed_size/(1024*1024.0) << " in MiB "
    << (double)compressed_size/(1024*1024*1024.0) << "in GiB "
    ;
}

template <class t_idx>
bool verify_index(collection& col, t_idx& idx)
{
    LOG(INFO) << "Verify that factorization is correct.";
    sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
    auto num_blocks = text.size() / t_idx::block_size;

    bool error = false;
    for (size_t i = 0; i < num_blocks; i++) {
        auto block_content = idx.block(i);
        auto block_start = i * t_idx::block_size;
        if (block_content.size() != t_idx::block_size) {
            error = true;
            LOG_N_TIMES(100, ERROR) << "Error in block " << i
                                    << " block size = " << block_content.size()
                                    << " encoding block_size = " << t_idx::block_size;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), text.begin() + block_start);
        if (!eq) {
            error = true;
            LOG(ERROR) << "BLOCK " << i << " NOT EQUAL";
            for (size_t j = 0; j < t_idx::block_size; j++) {
                if (text[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)text[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
            exit(EXIT_FAILURE);
        }
    }
    auto left = text.size() % t_idx::block_size;
    if (left) {
        auto block_content = idx.block(num_blocks);
        auto block_start = num_blocks * t_idx::block_size;
        if (block_content.size() != left) {
            error = true;
            LOG(ERROR) << "Error in  LAST block "
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
        return true;
    }
    return false;
}

template <class t_idx>
void output_stats(t_idx& idx,std::string name = std::string()) {
    LOG(INFO) << name << " text_size=" << idx.text_size;
    LOG(INFO) << name << " dict_size=" << idx.dict.size();
    LOG(INFO) << name << " dict_size_mb=" << idx.dict.size()/(1024*1024);
    LOG(INFO) << name << " type= " << idx.type();
    LOG(INFO) << name << " encoding_block_size=" << idx.encoding_block_size;
    LOG(INFO) << name << " encoding_size=" << idx.factor_text.size() / 8;
    LOG(INFO) << name << " encoding_size_mb=" << idx.factor_text.size() / (8*1024*1024.0);
    LOG(INFO) << name << " num_blocks = " << idx.block_map.num_blocks();

    /* compute blocksize stats */
    {
        std::vector<uint64_t> block_sizes(idx.block_map.num_blocks());
        std::adjacent_difference(idx.block_map.m_block_offsets.begin(),
            idx.block_map.m_block_offsets.end(),
            block_sizes.begin());
        std::sort(block_sizes.begin(),block_sizes.end());
        auto block_size_min = block_sizes[1] / 8; // bits to bytes
        auto block_size_max = block_sizes.back() / 8;  // bits to bytes
        auto block_size_sum = std::accumulate(block_sizes.begin(),block_sizes.end(), 0ULL);
        auto block_size_mean = (block_size_sum / (double) block_sizes.size()) / 8;
        auto block_size_med = block_sizes[(uint64_t)(block_sizes.size()*0.5)] / 8;
        auto block_size_1qrt = block_sizes[(uint64_t)(block_sizes.size()*0.25)] / 8;
        auto block_size_3qrt = block_sizes[(uint64_t)(block_sizes.size()*0.75)] / 8;
        LOG(INFO) << name << " block_sizes ";
        LOG(INFO) << " min=" << block_size_min
            << " 1.qrt=" << block_size_1qrt
            << " med=" << block_size_med
            << " mean=" << block_size_mean
            << " 3.qrt=" << block_size_3qrt
            << " max=" << block_size_max;
    }

    /* compute num factor stats */
    {
        std::vector<uint64_t> block_factors(idx.block_map.num_blocks());
        std::copy(idx.block_map.m_block_factors.begin(),idx.block_map.m_block_factors.end(),block_factors.begin());
        std::sort(block_factors.begin(),block_factors.end());
        auto block_factors_min = block_factors.front() / 8; // bits to bytes
        auto block_factors_max = block_factors.back() / 8;  // bits to bytes
        auto block_factors_sum = std::accumulate(block_factors.begin(),block_factors.end(), 0ULL);
        auto block_factors_mean = (block_factors_sum / (double) block_factors.size()) / 8;
        auto block_factors_med = block_factors[(uint64_t)(block_factors.size()*0.5)];
        auto block_factors_1qrt = block_factors[(uint64_t)(block_factors.size()*0.25)];
        auto block_factors_3qrt = block_factors[(uint64_t)(block_factors.size()*0.75)];
        LOG(INFO) << name << " block factors ";
        LOG(INFO) << " min=" << block_factors_min
            << " 1.qrt=" << block_factors_1qrt
            << " med=" << block_factors_med
            << " mean=" << block_factors_mean
            << " 3.qrt=" << block_factors_3qrt
            << " max=" << block_factors_max;

        LOG(INFO) << name << " bits_per_factor = " << idx.factor_text.size() / (double) block_factors_sum;
        auto space_of_bmap = sdsl::size_in_bytes(idx.block_map);
        auto space_on_disk = idx.factor_text.size() + (idx.dict.size()*8) + (space_of_bmap*8);
        LOG(INFO) << name << " space_savings = " << 100.0 * (1 - ((double)space_on_disk / ((double)idx.text_size*8))) << " %";
    }
    /* analyze factors */
    {
        auto num_literals = 0ULL;
        auto non_literals = 0ULL;
        auto num_factors = 0ULL;
        auto fitr = idx.factors_begin();
        auto fend = idx.factors_end();
        std::vector<uint64_t> flen_dist(idx.encoding_block_size);
        std::vector<uint64_t> dict_usage(idx.dict.size());
        while(fitr != fend) {
            num_factors++;
            auto fd = *fitr;
            if(fd.is_literal) num_literals++;
            else {
                flen_dist[fd.len]++;
                non_literals++;
                for(size_t i=0;i<fd.len;i++) {
                    dict_usage[fd.offset+i]++;
                }
            }
            ++fitr;
        }
        LOG(INFO) << name << " num_literals = " << num_literals;
        LOG(INFO) << name << " percent_literals = " << 100.0*num_literals / num_factors << " %";

        uint64_t flen_sum = 0;
        for(size_t i=0;i<flen_dist.size();i++) {
            flen_sum += i*flen_dist[i];
        }
        auto flen_mean = flen_sum / (double)non_literals;
        uint64_t flen_min = 0;
        uint64_t flen_max = 0;
        for(size_t i=0;i<flen_dist.size();i++) {
            if(flen_min == 0 && flen_dist[i] != 0) flen_min = i;
            if(flen_dist[i] != 0) flen_max = i;
        }
        uint64_t flen_1qrt_v = non_literals * 0.25;
        uint64_t flen_median_v = non_literals * 0.5;
        uint64_t flen_3qrt_v = non_literals * 0.75;

        uint64_t sum = 0;
        uint64_t flen_1qrt = 0;
        uint64_t flen_median = 0;
        uint64_t flen_3qrt = 0;
        for(size_t i=0;i<flen_dist.size();i++) {
            sum += flen_dist[i];
            if(sum >= flen_1qrt_v && flen_1qrt == 0) {
                flen_1qrt = i;
            }
            if(sum >= flen_median_v && flen_median == 0) {
                flen_median = i;
            }
            if(sum >= flen_3qrt_v && flen_3qrt == 0) {
                flen_3qrt = i;
            }
        }

        LOG(INFO) << name << " factor lens ";
        LOG(INFO) << " min=" << flen_min
            << " 1.qrt=" << flen_1qrt
            << " med=" << flen_median
            << " mean=" << flen_mean
            << " 3.qrt=" << flen_3qrt
            << " max=" << flen_max;

        LOG(INFO) << name << " dict usage ";
        double ds = idx.dict.size();
        auto num_zeros = std::count_if(dict_usage.begin(),dict_usage.end(), [](uint64_t i) {return i == 0;});
        LOG(INFO) << " b-0=" << num_zeros << " ("<<100*num_zeros/ds<<"%)";
        
        uint64_t thres = 1;
        uint64_t cnt = 1;
        while(cnt) {
            cnt = std::count_if(dict_usage.begin(),dict_usage.end(), [&thres](uint64_t i) {return i >= thres;});
            if(cnt) LOG(INFO) << " b-" << thres <<"="<<cnt<<" ("<<100*cnt/ds<<"%)";
            thres *= 2;
        }
    }
}


template <class t_idx>
void output_stats_csv(collection& col,t_idx& idx) {
    /* analyze factors */
    {
        auto num_literals = 0ULL;
        auto non_literals = 0ULL;
        auto num_factors = 0ULL;
        auto fitr = idx.factors_begin();
        auto fend = idx.factors_end();
        std::vector<uint64_t> flen_dist(idx.encoding_block_size);
        sdsl::int_vector<64> dict_usage(idx.dict.size());
        while(fitr != fend) {
            num_factors++;
            auto fd = *fitr;
            if(fd.is_literal) num_literals++;
            else {
                flen_dist[fd.len]++;
                non_literals++;
                for(size_t i=0;i<fd.len;i++) {
                    dict_usage[fd.offset+i]++;
                }
            }
            ++fitr;
        }

        LOG(INFO) << "FLEN;0;" << num_literals;
        for(size_t i=1;i<flen_dist.size();i++) {
            LOG(INFO) << "FLEN;"<<i<<";" << flen_dist[i];
        }

        double ds = idx.dict.size();
        auto num_zeros = std::count_if(dict_usage.begin(),dict_usage.end(), [](uint64_t i) {return i == 0;});
        LOG(INFO) << "DUSAGE;0;" << num_zeros << ";" << 100*num_zeros/ds;

        uint64_t thres = 1;
        uint64_t cnt = 1;
        while(cnt) {
            cnt = std::count_if(dict_usage.begin(),dict_usage.end(), [&thres](uint64_t i) {return i >= thres;});
            if(cnt) LOG(INFO) << "DUSAGE;" << thres << ";" << cnt << ";" << 100*cnt/ds;
            thres*=2;
        }

        {
            auto run_len = 0;
            for(size_t i=0;i<dict_usage.size();i++) {
                if(dict_usage[i] == 0) {
                    run_len++;
                } else {
                    if(run_len != 0) {
                        LOG(INFO) << "ZERORUN;" << run_len;
                    }
                    run_len = 0;
                }
            }
            if(run_len) {
                LOG(INFO) << "ZERORUN;" << run_len;
            }
        }


        std::vector<uint64_t> sample_hist(1024);
        for(size_t i=0;i<dict_usage.size();i++) {
            if(dict_usage[i] == 0) {
                auto pos_mod_1024 = i % 1024;
                sample_hist[pos_mod_1024]++;
            }
        }
        for(size_t i=0;i<sample_hist.size();i++) {
            LOG(INFO) << "sample_hist;" << i << ";" << sample_hist[i];
        }

    }
    /* analyze dict */
    LOG(INFO) << "analyze dict";
    auto num_bytes_in_dict = idx.dict.size();
    std::unordered_map<uint64_t,uint64_t> text_counts;
    {
        uint8_t* dptr = (uint8_t*) idx.dict.data();
        std::unordered_map<uint64_t,uint64_t> hash_counts;
        for(size_t i=0;i<num_bytes_in_dict-8;i++) {
            uint64_t* d64ptr = (uint64_t*)dptr;
            auto h = *d64ptr;
            hash_counts[h]++;
            text_counts[h] = 0;
            dptr++;
            LOG_EVERY_N(10*1024*1024,INFO) << "Processed " << i << " symbols of dict.";
        }
        {
            std::unordered_map<uint64_t,uint64_t> count_dists;
            for(const auto& hc: hash_counts) {
                auto cnt = hc.second;
                count_dists[cnt]++;
            }
            for(const auto& cd: count_dists) {
                LOG(INFO) << "D64C;" << cd.first << ";" << cd.second;
            }
        }
    }
    // /* check the text */
    // LOG(INFO) << "check text";
    // {
    //     text_counts.max_load_factor(0.3);
    //     LOG(INFO) << "text_counts.size() = " << text_counts.size();
    //     sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
    //     uint8_t* dptr = (uint8_t*) text.data();
    //     auto num_bytes_in_text = text.size();
    //     for(size_t i=0;i<num_bytes_in_text-8;i++) {
    //         uint64_t* d64ptr = (uint64_t*)dptr;
    //         auto h = *d64ptr;
    //         auto itr = text_counts.find(h);
    //         if( itr != text_counts.end()) itr->second++;
    //         dptr++;
    //         LOG_EVERY_N(100*1024*1024,INFO) << "Processed " << i << " symbols.";
    //     }
    //     LOG(INFO) << "Gather counts";
    //     std::unordered_map<uint64_t,uint64_t> count_dists;
    //     for(const auto& hc: text_counts) {
    //         auto cnt = hc.second;
    //         count_dists[cnt]++;
    //     }
    //     for(const auto& cd: count_dists) {
    //         LOG(INFO) << "T64C;" << cd.first << ";" << cd.second;
    //     }
    // }
}



// template <class t_idx_base1, class t_idx_base2,class t_idx_new>
template <class t_idx>
// void compare_indexes(collection& col, t_idx_base1& baseline1, t_idx_base2& baseline2, t_idx_new& new_idx)
void compare_indexes(collection& col, t_idx& idx, std::string s)
{
    /* (1) output encoding stats of baseline 1 */
    output_stats(idx, s);

    /* (2) output encoding stats of baseline 2 */
    // output_stats(baseline2,"baseline2");

    /* (3) output encoding stats of new */
    // output_stats(new_idx," new_idx");

    // /* (3) compress new_idx with baseline */
    // {
    //     col.param_map[PARAM_DICT_HASH] = baseline.m_dict_hash;
    //     typename t_idx_base::dictionary_index idx(col,false);
    //     auto itr = new_idx.dict.begin();
    //     auto end = new_idx.dict.end();
    //     auto factor_itr = idx.factorize(itr, end);
    //     auto num_literals = 0;
    //     auto non_literals = 0;
    //     auto num_factors = 0;
    //     std::vector<uint64_t> flen_dist(new_idx.dict.size());
    //     std::vector<uint64_t> dict_usage(baseline.dict.size());
    //     while (!factor_itr.finished()) {
    //         if (factor_itr.len == 0) {
    //             num_literals++;
    //         } else {
    //             auto offset = idx.sa[factor_itr.sp];
    //             auto len = factor_itr.len;
    //             flen_dist[len]++;
    //             for(size_t i=0;i<len;i++) {
    //                 dict_usage[offset+i]++;
    //             }
    //             non_literals++;
    //         }
    //         ++factor_itr;
    //         num_factors++;
    //     }
    //     LOG(INFO) << "==================================================";
    //     LOG(INFO) << "compress new_idx with baseline";
    //     LOG(INFO) << "    num_literals = " << num_literals;
    //     LOG(INFO) << "    non_literals = " << non_literals;
    //     LOG(INFO) << "    num_factors = " << num_factors;
    //     LOG(INFO) << "    percent_literals = " << 100.0*num_literals / num_factors << " %";

    //     uint64_t flen_sum = 0;
    //     for(size_t i=0;i<flen_dist.size();i++) {
    //         flen_sum += i*flen_dist[i];
    //     }
    //     auto flen_mean = flen_sum / (double)non_literals;
    //     uint64_t flen_min = 0;
    //     uint64_t flen_max = 0;
    //     for(size_t i=0;i<flen_dist.size();i++) {
    //         if(flen_min == 0 && flen_dist[i] != 0) flen_min = i;
    //         if(flen_dist[i] != 0) flen_max = i;
    //     }
    //     uint64_t flen_1qrt_v = non_literals * 0.25;
    //     uint64_t flen_median_v = non_literals * 0.5;
    //     uint64_t flen_3qrt_v = non_literals * 0.75;

    //     uint64_t sum = 0;
    //     uint64_t flen_1qrt = 0;
    //     uint64_t flen_median = 0;
    //     uint64_t flen_3qrt = 0;
    //     for(size_t i=0;i<flen_dist.size();i++) {
    //         sum += flen_dist[i];
    //         if(sum >= flen_1qrt_v && flen_1qrt == 0) {
    //             flen_1qrt = i;
    //         }
    //         if(sum >= flen_median_v && flen_median == 0) {
    //             flen_median = i;
    //         }
    //         if(sum >= flen_3qrt_v && flen_3qrt == 0) {
    //             flen_3qrt = i;
    //         }
    //     }

    //     LOG(INFO) << "    factor lens ";
    //     LOG(INFO) << " min=" << flen_min
    //         << " 1.qrt=" << flen_1qrt
    //         << " med=" << flen_median
    //         << " mean=" << flen_mean
    //         << " 3.qrt=" << flen_3qrt
    //         << " max=" << flen_max;

    //     LOG(INFO) << "    dict usage ";
    //     double ds = baseline.dict.size();
    //     auto num_zeros = std::count_if(dict_usage.begin(),dict_usage.end(), [](uint64_t i) {return i == 0;});
    //     LOG(INFO) << " b-0=" << num_zeros << " ("<<100*num_zeros/ds<<"%)";
        
    //     uint64_t thres = 1;
    //     uint64_t cnt = 1;
    //     while(cnt) {
    //         cnt = std::count_if(dict_usage.begin(),dict_usage.end(), [&thres](uint64_t i) {return i >= thres;});
    //         if(cnt) LOG(INFO) << " b-" << thres <<"="<<cnt<<" ("<<100*cnt/ds<<"%)";
    //         thres *= 2;
    //     }
    //     LOG(INFO);
    //     LOG(INFO) << "    estimated compressed size = " << (num_factors*5)/(1024*1024) << " MiB";
    // }
    // /* (4) compress baseline with new_idx */
    // {
    // }
}

template <class t_idx>
factorization_statistics dict_usage_stats(const t_idx& idx)
{
    factorization_statistics fs;
    fs.dict_usage.resize(idx.dict.size());
    fs.block_size = idx.encoding_block_size;
    auto fitr = idx.factors_begin();
    auto fend = idx.factors_end();
    for(size_t i=0;i<fs.dict_usage.size();i++) fs.dict_usage[i] = 0;
    while(fitr != fend) {
        auto fd = *fitr;
        if(!fd.is_literal) {
            for(size_t i=0;i<fd.len;i++) {
                fs.dict_usage[fd.offset+i]++;
            }
        }
        ++fitr;
    }
    return fs;
}