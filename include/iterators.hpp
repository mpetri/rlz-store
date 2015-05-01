#pragma once

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
        m_offset_buf.resize(m_idx.factorization_block_size);
        m_len_buf.resize(m_idx.factorization_block_size);
        decode_cur_block();
    }
    void decode_cur_block()
    {
        if( m_block_offset < m_idx.block_map.num_blocks() ) {
            m_factors_in_cur_block = m_idx.block_map.block_factors(m_block_offset);
            auto block_file_offset = m_idx.block_map.block_offset(m_block_offset);
            m_idx.decode_factors(block_file_offset, m_offset_buf, m_len_buf, m_factors_in_cur_block);
        }
    }
    value_type operator*()
    {
        if (m_factor_offset == 0) {
            decode_cur_block();
        }
        return { m_offset_buf[m_factor_offset], m_len_buf[m_factor_offset] };
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
    using value_type = factor_data;

private:
    t_idx& m_idx;
    size_t m_text_offset;
    size_t m_text_block_offset;
    size_t m_block_size;
    size_t m_block_offset;
    std::vector<uint32_t> m_offset_buf;
    std::vector<uint32_t> m_len_buf;
    std::vector<uint8_t> m_text_buf;
public:
    const size_t& block_id = m_block_offset;
    const size_t& block_size = m_block_size;
public:
    text_iterator(t_idx& idx, size_t text_offset)
        : m_idx(idx)
        , m_text_offset(text_offset)
        , m_text_block_offset(text_offset % m_idx.factorization_block_size)
        , m_block_size(m_idx.factorization_block_size)
        , m_block_offset(text_offset / m_idx.factorization_block_size)
    {
        m_offset_buf.resize(m_block_size);
        m_len_buf.resize(m_block_size);
        m_text_buf.resize(m_block_size);
    }
    inline void decode_cur_block()
    {
        m_block_size = m_idx.decode_block(m_block_offset, m_text_buf, m_offset_buf, m_len_buf);
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
};
