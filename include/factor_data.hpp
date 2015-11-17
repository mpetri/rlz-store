#pragma once

struct block_factor_data {
    std::vector<uint8_t> literals;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;
    std::vector<uint32_t> offset_literals; // combined offsets and literals for two-stream decoding
    size_t num_factors;
    size_t num_literals;
    size_t num_offsets;
    size_t num_offset_literals;
    bool last_factor_was_literal;

    block_factor_data() = default;
    block_factor_data(size_t block_size)
    {
        reset();
        resize(block_size);
    }

    void reset()
    {
        last_factor_was_literal = false;
        num_offsets = 0;
        num_factors = 0;
        num_literals = 0;
        num_offset_literals = 0;
    }

    void resize(size_t block_size)
    {
        literals.resize(block_size);
        offsets.resize(block_size);
        lengths.resize(block_size);
        offset_literals.resize(block_size);
    }

    template <class t_coder, class t_itr>
    void add_factor(t_coder& coder, t_itr text_itr, uint32_t offset, uint32_t len)
    {
        assert(len > 0); // we define len to be larger than 0 even for unknown syms.
        if (len <= coder.literal_threshold) {
            if (last_factor_was_literal && ((lengths[num_factors - 1] + len) <= coder.literal_threshold)) {
                // extend last literal
                lengths[num_factors - 1] += len;
            } else {
                lengths[num_factors++] = len;
            }
            std::copy(text_itr, text_itr + len, literals.begin() + num_literals);
            num_literals += len;
            last_factor_was_literal = true;
            // for two stream encoding we need both things combined!
            std::copy(text_itr, text_itr + len, offset_literals.begin() + num_offset_literals);
            num_offset_literals += len;
        } else {
            offsets[num_offsets] = offset;
            num_offsets++;
            last_factor_was_literal = false;
            lengths[num_factors++] = len;
            offset_literals[num_offset_literals++] = offset;
        }
    }
};
