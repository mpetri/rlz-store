#pragma once

#include "sdsl/int_vector_mapper.hpp"

struct factorization_info {
    uint64_t offset;
    uint64_t total_encoded_factors;
    uint64_t total_encoded_blocks;
    std::string factored_text_filename;
    std::string block_offset_filename;
    std::string block_factors_filename;
    bool operator<(const factorization_info& fi) const
    {
        return offset < fi.offset;
    }
};

struct factor_storage {
    uint64_t offset;
    uint64_t block_size;
    uint64_t total_encoded_factors = 0;
    uint64_t total_encoded_blocks = 0;
    uint64_t cur_factors_in_block = 0;
    hrclock::time_point encoding_start;
    std::vector<uint32_t> tmp_factor_offset_buffer;
    std::vector<uint32_t> tmp_factor_len_buffer;
    sdsl::int_vector_mapper<1> factored_text;
    sdsl::int_vector_mapper<0> block_offsets;
    sdsl::int_vector_mapper<0> block_factors;
    bit_ostream<sdsl::int_vector_mapper<1> > factor_stream;
    factor_storage(collection& col, size_t _block_size, size_t _offset)
        : offset(_offset)
        , block_size(_block_size)
        , factored_text(sdsl::write_out_buffer<1>::create(col.temp_file_name(KEY_FACTORIZED_TEXT, offset)))
        , block_offsets(sdsl::write_out_buffer<0>::create(col.temp_file_name(KEY_BLOCKOFFSETS, offset)))
        , block_factors(sdsl::write_out_buffer<0>::create(col.temp_file_name(KEY_BLOCKFACTORS, offset)))
        , factor_stream(factored_text)
    {
        // create a buffer we can write to without reallocating
        tmp_factor_offset_buffer.resize(block_size);
        tmp_factor_len_buffer.resize(block_size);
        // save the start of the encoding process
        encoding_start = hrclock::now();
    }
    void add_to_block_factor(uint32_t offset, uint32_t len)
    {
        tmp_factor_offset_buffer[cur_factors_in_block] = offset;
        tmp_factor_len_buffer[cur_factors_in_block] = len;
        cur_factors_in_block++;
    }
    void start_new_block()
    {
        cur_factors_in_block = 0;
    }
    template <class t_coder>
    void encode_current_block(t_coder& coder)
    {
        block_offsets.push_back(factor_stream.tellp());
        block_factors.push_back(cur_factors_in_block);

        total_encoded_factors += cur_factors_in_block;
        total_encoded_blocks++;
        coder.encode_block(factor_stream,
                           tmp_factor_offset_buffer,
                           tmp_factor_len_buffer,
                           cur_factors_in_block);
    }
    void output_stats(size_t total_blocks) const
    {
        auto cur_time = hrclock::now();
        auto time_spent = cur_time - encoding_start;
        auto time_in_sec = std::chrono::duration_cast<std::chrono::milliseconds>(time_spent).count() / 1000.0f;
        auto bytes_written = total_encoded_blocks * block_size;
        auto mb_written = bytes_written / (1024 * 1024);
        LOG(INFO) << "   (" << offset << ") "
                  << "FACTORS = " << total_encoded_factors << " "
                  << "BLOCKS = " << total_encoded_blocks << " "
                  << "F/B = " << (double)total_encoded_factors / (double)total_encoded_blocks << " "
                  << "SPEED = " << mb_written / time_in_sec << " MB/s"
                  << " (" << 100 * (double)total_encoded_blocks / (double)total_blocks << "%)";
    }

    factorization_info
    info() const
    {
        factorization_info fi;
        fi.offset = offset;
        fi.total_encoded_factors = total_encoded_factors;
        fi.total_encoded_blocks = total_encoded_blocks;
        fi.factored_text_filename = factored_text.file_name();
        fi.block_offset_filename = block_offsets.file_name();
        fi.block_factors_filename = block_factors.file_name();
        return fi;
    }
};

void
output_encoding_stats(std::vector<factorization_info>& efs, size_t n)
{
    uint64_t num_factors = 0;
    uint64_t num_blocks = 0;
    uint64_t num_bits = 0;
    for (const auto& fi : efs) {
        num_factors += fi.total_encoded_factors;
        num_blocks += fi.total_encoded_blocks;
        sdsl::int_vector_mapper<1, std::ios_base::in> block(fi.factored_text_filename);
        num_bits += block.size();
    }
    uint64_t nb = num_bits / 8;
    LOG(INFO) << "=====================================================================";
    LOG(INFO) << "text size          = " << n << " bytes (" << n / (1024 * 1024.0) << " MB)";
    LOG(INFO) << "encoding size      = " << nb << " bytes (" << nb / (1024 * 1024.0) << " MB)";
    LOG(INFO) << "compression ratio  = " << 100.0 * (((double)nb / (double)n)) << " %";
    LOG(INFO) << "space savings      = " << 100.0 * (1 - ((double)nb / (double)n)) << " %";
    LOG(INFO) << "number of factors  = " << num_factors;
    LOG(INFO) << "number of blocks   = " << num_blocks;
    LOG(INFO) << "avg factors/block  = " << (double)num_factors / (double)num_blocks;
    LOG(INFO) << "=====================================================================";
}

template<class t_fact_strategy>
void
merge_factor_encodings(collection& col, std::vector<factorization_info>& efs)
{
    auto dict_hash = col.param_map[PARAM_DICT_HASH];
    auto factor_file_name = t_fact_strategy::factor_file_name(col);
    auto boffsets_file_name = t_fact_strategy::boffsets_file_name(col);
    auto bfactors_file_name = t_fact_strategy::bfactors_file_name(col);
    // rename the first block to the correct file name
    std::sort(efs.begin(), efs.end());
    LOG(INFO) << "\tRename offset 0 block to output files";
    utils::rename_file(efs[0].factored_text_filename, factor_file_name);
    utils::rename_file(efs[0].block_offset_filename, boffsets_file_name);
    utils::rename_file(efs[0].block_factors_filename, bfactors_file_name);
    if (efs.size() != 1) { // append the rest and fix the offsets
        sdsl::int_vector_mapper<1> factored_text(factor_file_name);
        bit_ostream<sdsl::int_vector_mapper<1> > factor_stream(factored_text, factored_text.size());
        sdsl::int_vector_mapper<0> block_offsets(boffsets_file_name);
        sdsl::int_vector_mapper<0> block_factors(bfactors_file_name);

        for (size_t i = 1; i < efs.size(); i++) {
            LOG(INFO) << "\tCopy block " << i << " factors/offsets/counts";
            uint64_t foffset = factor_stream.tellp();
            {
                const sdsl::int_vector_mapper<1, std::ios_base::in> block_factor_text(efs[i].factored_text_filename);
                auto src_bits = block_factor_text.size();
                auto num_u64 = src_bits / 64;
                auto src_data = block_factor_text.data();
                factor_stream.write_int(src_data, num_u64, 64);
                // write leftover bits
                auto left = src_bits % 64;
                for (size_t j = src_bits - left; j < src_bits; j++) {
                    factor_stream.put(block_factor_text[j]);
                }
            }
            {
                // block offsets have to be adjusted to the appended position
                const sdsl::int_vector_mapper<0, std::ios_base::in> block_block_offsets(efs[i].block_offset_filename);
                for (const auto& off : block_block_offsets) {
                    block_offsets.push_back(off + foffset);
                }
            }
            const sdsl::int_vector_mapper<0, std::ios_base::in> block_block_factors(efs[i].block_factors_filename);
            for (const auto& nf : block_block_factors) {
                block_factors.push_back(nf);
            }
        }
        // delete all other files
        LOG(INFO) << "\tDelete temporary files";
        for (size_t i = 1; i < efs.size(); i++) {
            utils::remove_file(efs[i].factored_text_filename);
            utils::remove_file(efs[i].block_offset_filename);
            utils::remove_file(efs[i].block_factors_filename);
        }
    }
    // store file names in col map
    col.file_map[KEY_FACTORIZED_TEXT] = factor_file_name;
    col.file_map[KEY_BLOCKOFFSETS] = boffsets_file_name;
    col.file_map[KEY_BLOCKFACTORS] = bfactors_file_name;
}
