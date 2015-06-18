#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "iterators.hpp"
#include "block_maps.hpp"
#include "factor_selector.hpp"
#include "factorizor.hpp"
#include "factor_coder.hpp"
#include "dict_strategies.hpp"
#include "dict_indexes.hpp"

#include <sdsl/suffix_arrays.hpp>

#include <future>

using namespace std::chrono;

template <class t_coder,
          uint32_t t_block_size>
class lz_store_static {
public:
    using coder_type = t_coder;
    using block_map_type = block_map_uncompressed;
    using size_type = uint64_t;
private:
    sdsl::int_vector_mapper<1, std::ios_base::in> m_compressed_text;
    bit_istream<sdsl::int_vector_mapper<1, std::ios_base::in> > m_compressed_stream;
    block_map_type m_blockmap;
public:
    enum { block_size = t_block_size };
    uint64_t encoding_block_size = block_size;
    block_map_type& block_map = m_blockmap;
    coder_type coder;
    sdsl::int_vector_mapper<1, std::ios_base::in>& compressed_text = m_compressed_text;
    uint64_t text_size;
public:
    class builder;

    std::string type() const
    {
        return coder_type::type() + "-" + std::to_string(t_block_size);
    }

    lz_store_static() = delete;
    lz_store_static(lz_store_static&&) = default;
    lz_store_static& operator=(lz_store_static&&) = default;
    lz_store_static(collection& col)
        : m_compressed_text(col.file_map[KEY_LZ])
        , m_compressed_stream(m_compressed_text) // (1) mmap factored text
    {
        LOG(INFO) << "Loading Zlib store into memory (" << type() << ")";
        // (2) load the block map
        LOG(INFO) << "\tLoad block map";
        sdsl::load_from_file(m_blockmap, col.file_map[KEY_BLOCKMAP]);
        {
            LOG(INFO) << "\tDetermine text size";
            const sdsl::int_vector_mapper<8, std::ios_base::in> text(col.file_map[KEY_TEXT]);
            text_size = text.size();
        }
        LOG(INFO) << "Zlib store ready (" << type() << ")";
    }

    auto begin() const -> zlib_text_iterator<decltype(*this)>
    {
        return zlib_text_iterator<decltype(*this)>(*this, 0);
    }

    auto end() const -> zlib_text_iterator<decltype(*this)>
    {
        return zlib_text_iterator<decltype(*this)>(*this, size());
    }

    size_type size() const
    {
        return text_size;
    }

    inline uint64_t decode_block(uint64_t block_id, std::vector<uint8_t>& text, block_factor_data&) const
    {
        auto offset = m_blockmap.block_offset(block_id);
        m_compressed_stream.seek(offset);
        size_t out_size = block_size;
        if(block_id == m_blockmap.num_blocks() -1) {
            auto left = text_size % block_size;
            if(left != 0)
                out_size = left;
        }
        coder.decode(m_compressed_stream,text.data(),out_size);
        return out_size;
    }

    std::vector<uint8_t>
    block(const size_t block_id) const
    {
        std::vector<uint8_t> block_content(block_size);
        auto out_size = decode_block(block_id,block_content);
        if(out_size != block_size) block_content.resize(out_size);
        return block_content;
    }
};

template <class t_coder,
          uint32_t t_block_size
          >
class lz_store_static<t_coder,
                      t_block_size
                      >::builder {
public:
    using coder_type = t_coder;
    using block_map_type = block_map_uncompressed;
public:
    builder& set_rebuild(bool r)
    {
        rebuild = r;
        return *this;
    };
    builder& set_threads(uint8_t )
    {
        return *this;
    };
    builder& set_dict_size(uint64_t )
    {
        return *this;
    };

    static std::string blockmap_file_name(collection& col)
    {
        return col.path + "/index/" + KEY_BLOCKMAP + "-"
               + block_map_type::type() + "-" + std::to_string(t_block_size) 
               + "-" + coder_type::type()+".sdsl";
    }

    static std::string blockoffsets_file_name(collection& col)
    {
        return col.path + "/tmp/" + KEY_BLOCKOFFSETS + "-"
               + coder_type::type()+"-"+std::to_string(t_block_size) +".sdsl";
    }

    static std::string encoding_file_name(collection& col)
    {
        return col.path + "/index/" + KEY_LZ + "-"
               + block_map_type::type() + "-" + coder_type::type()+"-"+std::to_string(t_block_size)+".sdsl";
    }

    lz_store_static build_or_load(collection& col) const
    {
        auto start = hrclock::now();

        auto lz_file_name = encoding_file_name(col);
        auto bo_file_name = blockoffsets_file_name(col);
        if (rebuild || !utils::file_exists(lz_file_name)) {
            LOG(INFO) << "Encoding text (" << std::string(coder_type::type()+"-"+std::to_string(t_block_size)) << ")";
            auto start_enc = hrclock::now();
            const sdsl::int_vector_mapper<8, std::ios_base::in> text(col.file_map[KEY_TEXT]);
            auto encoded_text = sdsl::write_out_buffer<1>::create(lz_file_name);
            bit_ostream<sdsl::int_vector_mapper<1> > encoded_stream(encoded_text);
            auto block_offsets = sdsl::write_out_buffer<0>::create(bo_file_name);
            auto num_blocks = text.size() / t_block_size;
            auto left = text.size() % t_block_size;
            const uint8_t* data_ptr = (const uint8_t*) text.data();
            coder_type coder;
            for(size_t i=0;i<num_blocks;i++) {
                block_offsets.push_back(encoded_stream.tellp());
                coder.encode(encoded_stream,data_ptr,t_block_size);
                data_ptr += t_block_size;
            }
            if(left) {
                block_offsets.push_back(encoded_stream.tellp());
                coder.encode(encoded_stream,data_ptr,left);
                data_ptr += left;
            }
            auto bytes_written = encoded_stream.tellp() / 8;
            auto stop_enc = hrclock::now();
            auto text_size_mb = text.size() / (1024 * 1024.0);
            auto enc_seconds = duration_cast<milliseconds>(stop_enc - start_enc).count() / 1000.0;
            LOG(INFO) << "=====================================================================";
            LOG(INFO) << "text size          = " << text.size() << " bytes (" << text.size() / (1024 * 1024.0) << " MB)";
            LOG(INFO) << "encoding size      = " << bytes_written << " bytes (" << bytes_written / (1024 * 1024.0) << " MB)";
            LOG(INFO) << "compression ratio  = " << 100.0 * (((double)bytes_written / (double)text.size())) << " %";
            LOG(INFO) << "space savings      = " << 100.0 * (1 - ((double)bytes_written / (double)text.size())) << " %";
            LOG(INFO) << "number of blocks   = " << num_blocks;
            LOG(INFO) << "=====================================================================";
            LOG(INFO) << "Encoding time = " << enc_seconds << " sec";
            LOG(INFO) << "Encoding speed = " << text_size_mb / enc_seconds << " MB/s";
            LOG(INFO) << "Encoding done. (" << std::string(coder_type::type()+"-"+std::to_string(t_block_size)) << ")";
        } else {
            LOG(INFO) << "Encoded text exists.";
        }
        col.file_map[KEY_LZ] = lz_file_name;
        col.file_map[KEY_BLOCKOFFSETS] = bo_file_name;

        // (4) encode document start pos
        LOG(INFO) << "Create block map (" << block_map_type::type() << ")";
        auto blockmap_file = blockmap_file_name(col);
        if (rebuild || !utils::file_exists(blockmap_file)) {
            block_map_type tmp(col);
            sdsl::store_to_file(tmp, blockmap_file);
        }
        col.file_map[KEY_BLOCKMAP] = blockmap_file;

        auto stop = hrclock::now();
        LOG(INFO) << "LZ construction complete. time = " << duration_cast<seconds>(stop - start).count() << " sec";

        return lz_store_static(col);
    }

    lz_store_static load(collection& col) const
    {
        /* make sure components exists and register them */

        /* (2) check factorized text */
        auto enc_file_name = encoding_file_name(col);
        if (!utils::file_exists(enc_file_name)) {
            throw std::runtime_error("LOAD FAILED: Cannot find encoded text.");
        } else {
            col.file_map[KEY_LZ] = enc_file_name;
            col.file_map[KEY_BLOCKOFFSETS] = blockoffsets_file_name(col);
        }

        /* (2) check blockmap */
        auto blockmap_file = blockmap_file_name(col);
        if (!utils::file_exists(blockmap_file)) {
            throw std::runtime_error("LOAD FAILED: Cannot find blockmap.");
        } else {
            col.file_map[KEY_BLOCKMAP] = blockmap_file;
        }

        /* load */
        return lz_store_static(col);
    }
private:
    bool rebuild = false;
};
