#pragma once

struct block_factor_data {
    std::vector<uint8_t>  literals;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;
    size_t num_factors;
    size_t num_literals;
    size_t num_offsets;

    block_factor_data() = default;
    block_factor_data(size_t block_size) {
    	reset();
    	resize(block_size);
    }

    void reset() {
    	num_offsets = 0;
    	num_factors = 0;
    	num_literals = 0;
    }

    void resize(size_t block_size) {
    	literals.resize(block_size);
    	offsets.resize(block_size);
    	lengths.resize(block_size);
    }

    template<class t_coder,class t_itr>
    void add_factor(t_coder& coder,t_itr text_itr,uint32_t offset, uint32_t len)
    {
    	if(coder.literal_threshold <= len) {
    		std::copy(text_itr,text_itr+len,literals.begin()+num_literals);
    		num_literals += len;
    	} else {
    		offsets[num_offsets] = offset;
    		num_offsets++;
    	}
    	lengths[num_factors] = len;
    	num_factors++;
    }
};
