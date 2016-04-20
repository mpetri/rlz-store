#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "bit_coders.hpp"
#include "factor_data.hpp"

#include <sdsl/suffix_arrays.hpp>

struct coder_size_info {
    uint32_t literal_bytes = 0;
    uint32_t length_bytes = 0;
    uint32_t offset_bytes = 0;
};

/*
	encode factors in blocks.
 */
template <uint32_t t_literal_threshold = 1,
    class t_coder_literal = coder::fixed<32>,
    class t_coder_offset = coder::aligned_fixed<uint32_t>,
    class t_coder_len = coder::vbyte>
struct factor_coder_blocked {
    typedef typename sdsl::int_vector<>::size_type size_type;
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
    void encode_block(t_ostream& ofs, block_factor_data& bfd) const
    {
        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + bfd.num_factors, [](uint32_t& n) { n--; });
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        if (bfd.num_literals)
            literal_coder.encode(ofs, bfd.literals.data(), bfd.num_literals);
        if (bfd.num_offsets)
            offset_coder.encode(ofs, bfd.offsets.data(), bfd.num_offsets);
    }

    template <class t_istream>
    coder_size_info decode_block(t_istream& ifs, block_factor_data& bfd, size_t num_factors) const
    {
        coder_size_info csi;
        bfd.num_factors = num_factors;

        auto len_pos = ifs.tellg();
        len_coder.decode(ifs, bfd.lengths.data(), num_factors);
        csi.length_bytes = (ifs.tellg() - len_pos) / 8;

        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + num_factors, [](uint32_t& n) { n++; });
        bfd.num_literals = 0;
        auto num_literal_factors = 0;
        for (size_t i = 0; i < bfd.num_factors; i++) {
            if (bfd.lengths[i] <= literal_threshold) {
                bfd.num_literals += bfd.lengths[i];
                num_literal_factors++;
            }
        }
        if (bfd.num_literals) {
            auto lit_pos = ifs.tellg();
            literal_coder.decode(ifs, bfd.literals.data(), bfd.num_literals);
            csi.literal_bytes = (ifs.tellg() - lit_pos) / 8;
        }
        bfd.num_offsets = bfd.num_factors - num_literal_factors;
        if (bfd.num_offsets) {
            auto off_pos = ifs.tellg();
            offset_coder.decode(ifs, bfd.offsets.data(), bfd.num_offsets);
            csi.offset_bytes = (ifs.tellg() - off_pos) / 8;
        }
        return csi;
    }
};

/*
    encode factors in blocks as two streams.
 */
template <uint32_t t_literal_threshold = 1,
    class t_coder_offset = coder::aligned_fixed<uint32_t>,
    class t_coder_len = coder::vbyte>
struct factor_coder_blocked_twostream {
    typedef typename sdsl::int_vector<>::size_type size_type;
    enum { literal_threshold = t_literal_threshold };

private:
    t_coder_offset offsetliteral_coder;
    t_coder_len len_coder;
    std::string dummy = "abc";

public:
    static std::string type()
    {
        return "factor_coder_blocked_twostream-t=" + std::to_string(t_literal_threshold)
            + "-" + t_coder_offset::type() + "-" + t_coder_len::type();
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs, block_factor_data& bfd) const
    {
        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + bfd.num_factors, [](uint32_t& n) { n--; });
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        offsetliteral_coder.encode(ofs, bfd.offset_literals.data(), bfd.num_offset_literals);
    }

    template <class t_istream>
    void decode_block(t_istream& ifs, block_factor_data& bfd, size_t num_factors) const
    {
        bfd.num_factors = num_factors;
        len_coder.decode(ifs, bfd.lengths.data(), num_factors);
        offsetliteral_coder.decode(ifs, bfd.offset_literals.data(), num_factors);
        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + num_factors, [](uint32_t& n) { n++; });
        bfd.num_literals = 0;
        bfd.num_offsets = 0;
        for (size_t i = 0; i < bfd.num_factors; i++) {
            if (bfd.lengths[i] <= literal_threshold) {
                std::copy(bfd.offset_literals.begin() + i, bfd.offset_literals.begin() + i + bfd.lengths[i], bfd.literals.begin() + bfd.num_literals);
                bfd.num_literals += bfd.lengths[i];
            }
            else {
                bfd.offsets[bfd.num_offsets++] = bfd.offset_literals[i];
            }
        }
    }
};
