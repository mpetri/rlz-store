#pragma once

#include "sdsl/int_vector_mapper.hpp"

#include "factor_data.hpp"
#include "bit_streams.hpp"

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

struct factorization_statistics {
    using size_type = uint64_t;
    uint64_t block_size;
    uint64_t total_encoded_factors = 0;
    uint64_t total_encoded_blocks = 0;
    sdsl::int_vector<32> dict_usage;

    inline size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += dict_usage.serialize(out, child, "dict byte usage");
        written_bytes += sdsl::serialize(total_encoded_factors, out, child, "total factors");
        written_bytes += sdsl::serialize(block_size, out, child, "block_size");
        written_bytes += sdsl::serialize(total_encoded_blocks, out, child, "total encoded blocks");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        dict_usage.load(in);
        sdsl::read_member(total_encoded_factors, in);
        sdsl::read_member(block_size, in);
        sdsl::read_member(total_encoded_blocks, in);
    }
};

struct factor_tracker {
    using result_type = factorization_statistics;
    factorization_statistics fs;
    hrclock::time_point encoding_start;
    block_factor_data tmp_block_factor_data;
    size_t toffset;
    factor_tracker(collection& col, size_t _block_size, size_t _offset)
        : toffset(_offset)
    {
        {
            const sdsl::int_vector_mapper<8, std::ios_base::in> dict(col.file_map[KEY_DICT]);
            fs.dict_usage.resize(dict.size());
            for (size_t i = 0; i < dict.size(); i++)
                fs.dict_usage[i] = 0;
            fs.block_size = _block_size;
        }
        // create a buffer we can write to without reallocating
        tmp_block_factor_data.resize(_block_size);
        // save the start of the encoding process
        encoding_start = hrclock::now();
    }
    template <class t_coder, class t_itr>
    void add_to_block_factor(t_coder& coder, t_itr text_itr, uint32_t offset, uint32_t len)
    {
        tmp_block_factor_data.add_factor(coder, text_itr, offset, len);
    }
    void start_new_block()
    {
        tmp_block_factor_data.reset();
    }
    template <class t_coder>
    void encode_current_block(t_coder& coder)
    {
        size_t offsets_seen = 0;
        for (size_t i = 0; i < tmp_block_factor_data.num_factors; i++) {
            auto len = tmp_block_factor_data.lengths[i];
            if (len > coder.literal_threshold) {
                auto offset = tmp_block_factor_data.offsets[offsets_seen];
                for (size_t j = 0; j < len; j++) {
                    fs.dict_usage[offset + j]++;
                }
                offsets_seen++;
            }
        }
        fs.total_encoded_factors += tmp_block_factor_data.num_factors;
        fs.total_encoded_blocks++;
        tmp_block_factor_data.reset();
    }
    void output_stats(size_t total_blocks) const
    {
        auto cur_time = hrclock::now();
        auto time_spent = cur_time - encoding_start;
        auto time_in_sec = std::chrono::duration_cast<std::chrono::milliseconds>(time_spent).count() / 1000.0f;
        auto bytes_written = fs.total_encoded_blocks * fs.block_size;
        auto mb_written = bytes_written / (1024 * 1024);
        LOG(INFO) << "   (" << toffset << ") "
                  << "FACTORS = " << fs.total_encoded_factors << " "
                  << "BLOCKS = " << fs.total_encoded_blocks << " "
                  << "F/B = " << (double)fs.total_encoded_factors / (double)fs.total_encoded_blocks << " "
                  << "SPEED = " << mb_written / time_in_sec << " MB/s"
                  << " (" << 100 * (double)fs.total_encoded_blocks / (double)total_blocks << "%)";
    }

    factorization_statistics
    result() const
    {
        return fs;
    }
};

void output_encoding_stats(collection&, std::vector<factorization_statistics>&)
{
}

template <class t_fact_strategy>
factorization_statistics
merge_factor_encodings(collection&, std::vector<factorization_statistics>& efs)
{
    factorization_statistics fs;
    fs.block_size = efs[0].block_size;
    fs.dict_usage.resize(efs[0].dict_usage.size());
    for (size_t i = 0; i < efs.size(); i++) {
        for (size_t j = 0; j < efs[i].dict_usage.size(); j++) {
            fs.dict_usage[j] += efs[i].dict_usage[j];
        }
    }
    return fs;
}

struct factor_storage {
    using result_type = factorization_info;
    uint64_t toffset;
    uint64_t block_size;
    uint64_t total_encoded_factors = 0;
    uint64_t total_encoded_blocks = 0;
    uint64_t blocks_encoded_since_last_stats_output = 0;
    uint64_t factors_encoded_since_last_stats_output = 0;
    hrclock::time_point encoding_start;
    hrclock::time_point last_stat_output;
    block_factor_data tmp_block_factor_data;
    sdsl::int_vector_mapper<1> factored_text;
    sdsl::int_vector_mapper<0> block_offsets;
    sdsl::int_vector_mapper<0> block_factors;
    bit_ostream<sdsl::int_vector_mapper<1> > factor_stream;
    factor_storage(collection& col, size_t _block_size, size_t _offset)
        : toffset(_offset)
        , block_size(_block_size)
        , factored_text(sdsl::write_out_buffer<1>::create(col.temp_file_name(KEY_FACTORIZED_TEXT, toffset)))
        , block_offsets(sdsl::write_out_buffer<0>::create(col.temp_file_name(KEY_BLOCKOFFSETS, toffset)))
        , block_factors(sdsl::write_out_buffer<0>::create(col.temp_file_name(KEY_BLOCKFACTORS, toffset)))
        , factor_stream(factored_text)
    {
        // create a buffer we can write to without reallocating
        tmp_block_factor_data.resize(block_size);
        // save the start of the encoding process
        encoding_start = hrclock::now();
        last_stat_output = hrclock::now();
    }
    template <class t_coder, class t_itr>
    void add_to_block_factor(t_coder& coder, t_itr text_itr, uint32_t offset, uint32_t len)
    {
        tmp_block_factor_data.add_factor(coder, text_itr, offset, len);
    }
    void start_new_block()
    {
        tmp_block_factor_data.reset();
    }
    template <class t_coder>
    void encode_current_block(t_coder& coder)
    {
        block_offsets.push_back(factor_stream.tellp());
        block_factors.push_back(tmp_block_factor_data.num_factors);
        total_encoded_factors += tmp_block_factor_data.num_factors;
        factors_encoded_since_last_stats_output  += tmp_block_factor_data.num_factors;
        total_encoded_blocks++;
        blocks_encoded_since_last_stats_output++;
        coder.encode_block(factor_stream, tmp_block_factor_data);
    }
    void output_stats(size_t total_blocks) 
    {
        auto cur_time = hrclock::now();
        auto time_spent = cur_time - last_stat_output;
        last_stat_output = cur_time;
        auto time_in_sec = std::chrono::duration_cast<std::chrono::milliseconds>(time_spent).count() / 1000.0f;
        auto bytes_written = blocks_encoded_since_last_stats_output * block_size;
        auto mb_written = bytes_written / (1024 * 1024);
        LOG(INFO) << "   (" << toffset << ") "
                  << "FACTORS = " << factors_encoded_since_last_stats_output << " "
                  << "BLOCKS = " << total_encoded_blocks << " "
                  << "F/B = " << (double)factors_encoded_since_last_stats_output / (double)blocks_encoded_since_last_stats_output << " "
                  << "Total F/B = " << (double)total_encoded_factors / (double)total_encoded_blocks << " "
                  << "SPEED = " << mb_written / time_in_sec << " MiB/s"
                  << " (" << 100 * (double)total_encoded_blocks / (double)total_blocks << "%)";
        factors_encoded_since_last_stats_output = 0;
        blocks_encoded_since_last_stats_output = 0;
    }

    factorization_info
    result() const
    {
        factorization_info fi;
        fi.offset = toffset;
        fi.total_encoded_factors = total_encoded_factors;
        fi.total_encoded_blocks = total_encoded_blocks;
        fi.factored_text_filename = factored_text.file_name();
        fi.block_offset_filename = block_offsets.file_name();
        fi.block_factors_filename = block_factors.file_name();
        return fi;
    }
};

void output_encoding_stats(collection& col, std::vector<factorization_info>& efs)
{
    size_t text_size_bytes = 0;
    {
        const sdsl::int_vector_mapper<8, std::ios_base::in> text(col.file_map[KEY_TEXT]);
        text_size_bytes = text.size();
    }
    size_t dict_size_bytes = 0;
    {
        const sdsl::int_vector_mapper<8, std::ios_base::in> dict(col.file_map[KEY_DICT]);
        dict_size_bytes = dict.size();
    }

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
    // add dict size to encoding
    nb += dict_size_bytes;
    LOG(INFO) << "=====================================================================";
    LOG(INFO) << "text size          = " << text_size_bytes << " bytes (" << text_size_bytes / (1024 * 1024.0) << " MB)";
    LOG(INFO) << "encoding size      = " << nb << " bytes (" << nb / (1024 * 1024.0) << " MB)";
    LOG(INFO) << "compression ratio  = " << 100.0 * (((double)nb / (double)text_size_bytes)) << " %";
    LOG(INFO) << "space savings      = " << 100.0 * (1 - ((double)nb / (double)text_size_bytes)) << " %";
    LOG(INFO) << "number of factors  = " << num_factors;
    LOG(INFO) << "bits per factor    = " << (double)(8 * nb) / (double)num_factors;
    LOG(INFO) << "number of blocks   = " << num_blocks;
    LOG(INFO) << "avg factors/block  = " << (double)num_factors / (double)num_blocks;
    LOG(INFO) << "=====================================================================";
}

template <class t_fact_strategy>
factorization_info
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

    factorization_info fi;
    return fi;
}
