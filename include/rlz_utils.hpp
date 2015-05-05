#pragma once

#include <chrono>

#include "utils.hpp"

using namespace std::chrono;

template<class t_idx>
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
    auto factors_mb = idx.factor_text.size() / (double)(8*1024*1024); // bits to mb
    LOG(INFO) << "decoding speed = " << factors_mb / fact_seconds << " MB/s";
}

template<class t_idx>
void benchmark_text_decoding(const t_idx& idx)
{
    LOG(INFO) << "Measure text decoding speed (" << idx.type() << ")";
    auto start = hrclock::now();
    auto itr = idx.begin();
    auto end = idx.end();

    size_t checksum = 0;
    size_t num_syms = 0;
    auto n = idx.size();
    for(size_t i=0;i<n;i++) {
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
    auto num_blocks = num_syms / idx.factorization_block_size;
    LOG(INFO) << "num blocks = " << num_blocks;
    LOG(INFO) << "blocks per sec = " << num_blocks / text_seconds;
}

template <class t_idx>
bool verify_index(collection& col,t_idx& idx) {
    LOG(INFO) << "Verify that factorization is correct.";
    sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
    auto num_blocks = text.size() / idx.factorization_block_size;

    bool error = false;
    for (size_t i = 0; i < num_blocks; i++) {
        auto block_content = idx.block(i);
        auto block_start = i * idx.factorization_block_size;
        if (block_content.size() != idx.factorization_block_size) {
            error = true;
            LOG_N_TIMES(100, ERROR) << "Error in block " << i
                                    << " block size = " << block_content.size()
                                    << " factorization_block_size = " << idx.factorization_block_size;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), text.begin() + block_start);
        if (!eq) {
            error = true;
            LOG(ERROR) << "BLOCK " << i << " NOT EQUAL";
            for (size_t j = 0; j < idx.factorization_block_size; j++) {
                if (text[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)text[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
            exit(EXIT_FAILURE);
        }
    }
    auto left = text.size() % idx.factorization_block_size;
    if (left) {
        auto block_content = idx.block(num_blocks);
        auto block_start = num_blocks * idx.factorization_block_size;
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
