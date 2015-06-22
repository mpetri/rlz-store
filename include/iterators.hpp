#pragma once

#include "factor_data.hpp"

struct factor_data {
    bool is_literal;
    uint8_t* literal_ptr;
    uint64_t offset;
    uint64_t len;
};

template <class t_idx>
class factor_iterator {
public:
    using size_type = uint64_t;
    using value_type = factor_data;

private:
    t_idx& m_idx;
    size_t m_block_offset;
    size_t m_factor_offset;
    size_t m_factors_in_cur_block;
    size_t m_in_block_literals_offset;
    size_t m_in_block_offsets_offset;
    block_factor_data m_block_factor_data;
    std::vector<uint32_t> m_offset_buf;
    std::vector<uint32_t> m_len_buf;

public:
    const size_t& block_id = m_block_offset;

public:
    factor_iterator(t_idx& idx, size_t block_offset, size_t factor_offset)
        : m_idx(idx)
        , m_block_offset(block_offset)
        , m_factor_offset(factor_offset)
    {
        m_block_factor_data.resize(m_idx.encoding_block_size);
        decode_cur_block();
    }
    void decode_cur_block()
    {
        if (m_block_offset < m_idx.block_map.num_blocks()) {
            m_factors_in_cur_block = m_idx.block_map.block_factors(m_block_offset);
            auto block_file_offset = m_idx.block_map.block_offset(m_block_offset);
            m_idx.decode_factors(block_file_offset, m_block_factor_data, m_factors_in_cur_block);
            m_in_block_literals_offset = 0;
            m_in_block_offsets_offset = 0;
        }
    }
    value_type operator*()
    {
        if (m_factor_offset == 0) {
            decode_cur_block();
        }
        factor_data fd;
        fd.len = m_block_factor_data.lengths[m_factor_offset];
        if( m_block_factor_data.lengths[m_factor_offset] <= m_idx.factor_coder.literal_threshold ) {
            /* literal factor */
            fd.is_literal = true;
            fd.literal_ptr = m_block_factor_data.literals.data() + m_in_block_literals_offset;
            m_in_block_literals_offset += fd.len;
        } else {
            fd.is_literal = false;
            fd.offset = m_block_factor_data.offsets[m_in_block_offsets_offset];
            m_in_block_offsets_offset++;
        }
        return fd;
    }
    bool operator==(const factor_iterator& b) const
    {
        return m_block_offset == b.m_block_offset && m_factor_offset == b.m_factor_offset;
    }
    bool operator!=(const factor_iterator& b) const
    {
        return m_block_offset != b.m_block_offset || m_factor_offset != b.m_factor_offset;
    }
    factor_iterator& operator++()
    {
        if (m_factor_offset + 1 == m_factors_in_cur_block) {
            m_block_offset++;
            m_factor_offset = 0;
        } else {
            m_factor_offset++;
        }
        return *this;
    }
};

template <class t_idx>
class text_iterator {
public:
    using size_type = uint64_t;
private:
    t_idx& m_idx;
    size_t m_text_offset;
    size_t m_text_block_offset;
    size_t m_block_size;
    size_t m_block_offset;
    block_factor_data m_block_factor_data;
    std::vector<uint8_t> m_text_buf;

public:
    const size_t& block_id = m_block_offset;
    const size_t& block_size = m_block_size;

public:
    text_iterator(t_idx& idx, size_t text_offset)
        : m_idx(idx)
        , m_text_offset(text_offset)
        , m_text_block_offset(text_offset % m_idx.encoding_block_size)
        , m_block_size(m_idx.encoding_block_size)
        , m_block_offset(text_offset / m_idx.encoding_block_size)
    {
        m_block_factor_data.resize(m_block_size);
        m_text_buf.resize(m_block_size);
    }
    inline void decode_cur_block()
    {
        m_block_size = m_idx.decode_block(m_block_offset, m_text_buf, m_block_factor_data);
    }
    inline uint8_t operator*()
    {
        if (m_text_block_offset == 0) {
            decode_cur_block();
        }
        return m_text_buf[m_text_block_offset];
    }
    inline bool operator==(const text_iterator& b) const
    {
        return m_text_offset == b.m_text_offset;
    }
    inline bool operator!=(const text_iterator& b) const
    {
        return m_text_offset != b.m_text_offset;
    }
    inline text_iterator& operator++()
    {
        if (m_text_block_offset + 1 == m_block_size) {
            m_text_block_offset = 0;
            m_block_offset++;
        } else {
            m_text_block_offset++;
        }
        m_text_offset++;
        return *this;
    }

    void seek(size_type new_text_offset) {
        auto new_block_offset = new_text_offset / m_block_size;
        m_text_block_offset = new_text_offset % m_block_size;
        if(new_block_offset != m_block_offset) {
            m_block_offset = new_block_offset;
            if(m_text_block_offset != 0) decode_cur_block();
        }
    }
};


template <class t_idx>
class zlib_text_iterator {
public:
    using size_type = uint64_t;
private:
    t_idx& m_idx;
    size_t m_text_offset;
    size_t m_text_block_offset;
    size_t m_block_size;
    size_t m_block_offset;
    std::vector<uint8_t> m_text_buf;
    block_factor_data m_block_factor_data;
public:
    const size_t& block_id = m_block_offset;
    const size_t& block_size = m_block_size;
public:
    zlib_text_iterator(t_idx& idx, size_t text_offset)
        : m_idx(idx)
        , m_text_offset(text_offset)
        , m_text_block_offset(text_offset % m_idx.encoding_block_size)
        , m_block_size(m_idx.encoding_block_size)
        , m_block_offset(text_offset / m_idx.encoding_block_size)
    {
        m_text_buf.resize(m_block_size);
    }
    inline void decode_cur_block()
    {
        m_block_size = m_idx.decode_block(m_block_offset, m_text_buf,m_block_factor_data);
    }
    inline uint8_t operator*()
    {
        if (m_text_block_offset == 0) {
            decode_cur_block();
        }
        return m_text_buf[m_text_block_offset];
    }
    inline bool operator==(const zlib_text_iterator& b) const
    {
        return m_text_offset == b.m_text_offset;
    }
    inline bool operator!=(const zlib_text_iterator& b) const
    {
        return m_text_offset != b.m_text_offset;
    }
    inline zlib_text_iterator& operator++()
    {
        if (m_text_block_offset + 1 == m_block_size) {
            m_text_block_offset = 0;
            m_block_offset++;
        } else {
            m_text_block_offset++;
        }
        m_text_offset++;
        return *this;
    }

    void seek(size_type new_text_offset) {
        auto new_block_offset = new_text_offset / m_block_size;
        m_text_block_offset = new_text_offset % m_block_size;
        if(new_block_offset != m_block_offset) {
            m_block_offset = new_block_offset;
            if(m_text_block_offset != 0) decode_cur_block();
        }
    }
};
