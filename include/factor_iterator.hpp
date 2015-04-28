#pragma once

template<class t_idx>
class factor_iterator {
	t_idx& m_idx;
	size_t m_block_offset;
	size_t m_factor_offset;
	size_t m_factors_in_cur_block;
	std::vector<factor_data> m_buf;
public:
	factor_iterator(t_idx& idx,size_t block_offset,size_t factor_offset)
		: m_idx(idx), m_block_offset(block_offset), m_factor_offset(factor_offset)
	{
		m_buf.resize(m_idx.factorization_block_size);
		decode_cur_block();
	}
	void decode_cur_block() {
		m_factors_in_cur_block = m_idx.block_map.block_factors(m_block_offset);
		auto block_file_offset = m_idx.block_map.block_offset(m_block_offset);
		m_idx.decode_factors(block_file_offset,m_factors_in_cur_block,m_buf);
	}
	factor_data operator*() {
		if(m_factor_offset == 0) {
			decode_cur_block();
		}
		return m_buf[m_factor_offset];
	}
    bool operator ==(const factor_iterator& b) const
    {
        return m_block_offset == b.m_block_offset && m_factor_offset == b.m_factor_offset;
    }
    bool operator !=(const factor_iterator& b) const
    {
        return m_block_offset != b.m_block_offset || m_factor_offset != b.m_factor_offset;
    }
    factor_iterator& operator++()
    {
    	if( m_factor_offset+1 == m_factors_in_cur_block ) {
    		m_block_offset++;
    		m_factor_offset = 0;
    	} else {
    		m_factor_offset++;
    	}
        return *this;
    }
};

