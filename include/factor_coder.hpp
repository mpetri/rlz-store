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
    enum { literal_threshold = t_literal_threshold };
    t_coder_literal literal_coder;
    t_coder_offset offset_coder;
    t_coder_len len_coder;
    static std::string type()
    {
        return "factor_coder_blocked-t=" + std::to_string(t_literal_threshold) 
            + "-" + t_coder_literal::type() + "-" + t_coder_offset::type() + "-" + t_coder_len::type();
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs,block_factor_data& bfd) const
    {
        std::for_each(bfd.lengths.begin(),bfd.lengths.begin()+bfd.num_factors, [](uint32_t &n){ n--; });
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        if(bfd.num_literals) literal_coder.encode(ofs, bfd.literals.data(), bfd.num_literals);
        if(bfd.num_offsets) offset_coder.encode(ofs, bfd.offsets.data(), bfd.num_offsets);
    }

    template <class t_istream>
    void decode_block(t_istream& ifs, block_factor_data& bfd, size_t num_factors) const
    {
        bfd.num_factors = num_factors;
        len_coder.decode(ifs, bfd.lengths.data(), num_factors);
        std::for_each(bfd.lengths.begin(),bfd.lengths.begin()+num_factors, [](uint32_t &n){ n++; });
        bfd.num_literals = 0;
        auto num_literal_factors = 0;
        for(size_t i=0;i<bfd.num_factors;i++) {
            if(bfd.lengths[i] <= literal_threshold) {
                bfd.num_literals += bfd.lengths[i];
                num_literal_factors++;
            }
        }
        if(bfd.num_literals) literal_coder.decode(ifs, bfd.literals.data(), bfd.num_literals);
        bfd.num_offsets = bfd.num_factors - num_literal_factors;
        if(bfd.num_offsets) offset_coder.decode(ifs, bfd.offsets.data(), bfd.num_offsets);
    }
};
