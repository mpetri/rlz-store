#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "bit_coders.hpp"

#include <sdsl/suffix_arrays.hpp>

struct factor_data {
    uint32_t offset;
    uint32_t len;
};

template <class t_coder_offset = coder::u32,
          class t_coder_len = coder::vbyte>
struct factor_coder {
    t_coder_offset offset_coder;
    t_coder_len len_coder;
    static std::string type()
    {
        return "factor_coder-" + t_coder_offset::type() + "-" + t_coder_len::type();
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs,
                      const std::vector<uint32_t>& offsets,
                      const std::vector<uint32_t>& lens,
                      size_t num_factors) const
    {
        for (size_t i = 0; i < num_factors; i++) {
            offset_coder.encode_check_size(ofs, offsets[i]);
            len_coder.encode_check_size(ofs, lens[i]);
        }
    }

    template <class t_istream>
    void decode_block(t_istream& ifs,
                      std::vector<uint32_t>& offsets,
                      std::vector<uint32_t>& lens,
                      size_t num_factors) const
    {
        for (size_t i = 0; i < num_factors; i++) {
            offsets[i] = offset_coder.decode(ifs);
            lens[i] = len_coder.decode(ifs);
        }
    }
};

/*
	encode factors in blocks.
 */
template <class t_coder_offset = coder::u32,
          class t_coder_len = coder::vbyte>
struct factor_coder_blocked {
    t_coder_offset offset_coder;
    t_coder_len len_coder;
    static std::string type()
    {
        return "factor_coder_blocked-" + t_coder_offset::type() + "-" + t_coder_len::type();
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs,
                      const std::vector<uint32_t>& offsets,
                      const std::vector<uint32_t>& lens,
                      size_t num_factors) const
    {
        offset_coder.encode(ofs, std::begin(offsets), std::begin(offsets) + num_factors);
        len_coder.encode(ofs, std::begin(lens), std::begin(lens) + num_factors);
    }

    template <class t_istream>
    void decode_block(t_istream& ifs,
                      std::vector<uint32_t>& offsets,
                      std::vector<uint32_t>& lens,
                      size_t num_factors) const
    {
        offset_coder.decode(ifs, std::begin(offsets), num_factors);
        len_coder.decode(ifs, std::begin(lens), num_factors);
    }
};
