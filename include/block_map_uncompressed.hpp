#pragma once

#include <sdsl/int_vector.hpp>
#include <string>

struct block_map_uncompressed {
    typedef typename sdsl::int_vector<>::size_type size_type;
    sdsl::int_vector<> m_block_offsets;
    sdsl::int_vector<> m_block_factors;

    static std::string type()
    {
        return "block_map_uncompressed";
    }

    block_map_uncompressed() = default;
    block_map_uncompressed(block_map_uncompressed&&) = default;

    block_map_uncompressed(collection& col)
    {
        LOG(INFO) << "\tLoad block offsets from file";
        sdsl::load_from_file(m_block_offsets, col.file_map[KEY_BLOCKOFFSETS]);
        if(col.file_map.find(KEY_BLOCKFACTORS) != col.file_map.end()) {
            sdsl::load_from_file(m_block_factors, col.file_map[KEY_BLOCKFACTORS]);
            sdsl::util::bit_compress(m_block_factors);
        }
        sdsl::util::bit_compress(m_block_offsets);
    }

    inline size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += m_block_offsets.serialize(out, child, "offsets");
        written_bytes += m_block_factors.serialize(out, child, "num_factors");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        m_block_offsets.load(in);
        m_block_factors.load(in);
    }

    inline size_type block_offset(size_t block_id) const
    {
        return m_block_offsets[block_id];
    }
    inline size_type block_factors(size_t block_id) const
    {
        return m_block_factors[block_id];
    }

    inline size_type num_blocks() const
    {
        return m_block_offsets.size();
    }
};
