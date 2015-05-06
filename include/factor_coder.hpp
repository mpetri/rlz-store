#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "bit_coders.hpp"
#include "factor_data.hpp"

#include <sdsl/suffix_arrays.hpp>

/*
	encode factors in blocks.
 */
template <uint32_t t_literal_threshold = 1,
          class t_coder_literal = coder::fixed<32>,
          class t_coder_offset = coder::aligned_fixed<uint32_t>,
          class t_coder_len = coder::vbyte>
struct factor_coder_blocked {
    uint32_t literal_threshold = t_literal_threshold;
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
      bfd.num_factors = num_factors;
        len_coder.decode(ifs,bfd.lengths.data(), num_factors);
        bfd.num_literals = std::count_if(bfd.lengths.begin(),
          bfd.lengths.begin()+num_factors, [](uint32_t i) {return i <= t_literal_threshold;});
        literal_coder.decode(ifs,bfd.literals.data(),num_literals);
        bfd.num_offsets = num_factors - num_literals;
        offset_coder.decode(ifs,bfd.offsets.data(),num_offsets);
    }
};

