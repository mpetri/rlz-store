#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "bit_coders.hpp"

#include <sdsl/suffix_arrays.hpp>

struct block_factor_data {
    std::vector<uint8_t>  literals;
    std::vector<uint32_t> offsets;
    std::vector<uint32_t> lengths;
    size_t num_factors;
    size_t num_literals;
    size_t num_offsets;
};

/*
	encode factors in blocks.
 */
template <uint32_t t_literal_threshold = 3,
          class t_coder_literal = coder::u32,
          class t_coder_offset = coder::u32,
          class t_coder_len = coder::vbyte>
struct factor_coder_blocked {
    t_coder_literal literal_coder;
    t_coder_offset offset_coder;
    t_coder_len len_coder;
    static std::string type()
    {
        return "factor_coder_blocked-" + t_coder_offset::type() + "-" + t_coder_offset::type() + "-" + t_coder_len::type();
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs,const block_factor_data& bfd) const
    {
        len_coder.encode(ofs,bfd.lengths.data(),bfd.num_factors);
        literal_coder.encode(ofs,bfd.literals.data(),bfd.num_literals);
        offset_coder.encode(ofs,bfd.offsets.data(),bfd.num_offsets);
    }

    template <class t_istream>
    void decode_block(t_istream& ifs,block_factor_data& bfd,size_t num_factors) const
    {
        len_coder.decode(ifs,bfd.lengths.data(), num_factors);
        auto num_literals = std::count_if(v.begin(), v.end(), [](uint32_t i) {return i <= t_literal_threshold;});
        literal_coder.decode(ofs,bfd.literals.data(),num_literals);
        auto num_offsets = num_factors - num_literals;
        offset_coder.decode(ifs,bfd.offsets.data(),num_offsets);
    }
};

