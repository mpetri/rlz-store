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
    typedef typename sdsl::int_vector<>::size_type size_type;
    enum { literal_threshold = t_literal_threshold };
    t_coder_literal literal_coder;
    t_coder_offset offset_coder;
    t_coder_len len_coder;
    enum { prime_bytes = 0 };
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

    inline size_type serialize(std::ostream& , sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& )
    {
    }

    size_type size_in_bytes() const {
        return sdsl::size_in_bytes(*this);
    }

    void set_offset_prime(const sdsl::int_vector<32>& ) {
    }

    void set_length_prime(const sdsl::int_vector<32>& ) {
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
    enum { prime_bytes = 0 };
    static std::string type()
    {
        return "factor_coder_blocked_twostream-t=" + std::to_string(t_literal_threshold) 
            + "-" + t_coder_offset::type() + "-" + t_coder_len::type();
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs,block_factor_data& bfd) const
    {
        std::for_each(bfd.lengths.begin(),bfd.lengths.begin()+bfd.num_factors, [](uint32_t &n){ n--; });
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        offsetliteral_coder.encode(ofs, bfd.offset_literals.data(), bfd.num_offset_literals);
    }

    template <class t_istream>
    void decode_block(t_istream& ifs, block_factor_data& bfd, size_t num_factors) const
    {
        bfd.num_factors = num_factors;
        len_coder.decode(ifs, bfd.lengths.data(), num_factors);
        offsetliteral_coder.decode(ifs, bfd.offset_literals.data(), num_factors);
        std::for_each(bfd.lengths.begin(),bfd.lengths.begin()+num_factors, [](uint32_t &n){ n++; });
        bfd.num_literals = 0;
        bfd.num_offsets = 0;
        for(size_t i=0;i<bfd.num_factors;i++) {
            if(bfd.lengths[i] <= literal_threshold) {
                std::copy(bfd.offset_literals.begin()+i,bfd.offset_literals.begin()+i+bfd.lengths[i],bfd.literals.begin()+bfd.num_literals);
                bfd.num_literals += bfd.lengths[i];
            } else {
                bfd.offsets[bfd.num_offsets++] = bfd.offset_literals[i];
            }
        }
    }

    inline size_type serialize(std::ostream& , sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& )
    {
    }

    size_type size_in_bytes() const {
        return sdsl::size_in_bytes(*this);
    }

    void set_offset_prime(const sdsl::int_vector<32>& ) {
    }

    void set_length_prime(const sdsl::int_vector<32>& ) {
    }
};

template <
uint32_t t_literal_threshold = 1,
class t_coder_offset = coder::aligned_fixed<uint32_t>,
class t_coder_len = coder::vbyte,
uint32_t t_prime_bytes = 32*1024
>
struct factor_coder_blocked_twostream_primed {
    typedef typename sdsl::int_vector<>::size_type size_type;
    enum { literal_threshold = t_literal_threshold };
    t_coder_offset offsetliteral_coder;
    sdsl::int_vector<32> m_offset_prime;
    sdsl::int_vector<32> m_length_prime;
    t_coder_len len_coder;
    enum { prime_bytes = t_prime_bytes };
    static std::string type()
    {
        return "factor_coder_blocked_twostream_primed-t=" + std::to_string(t_literal_threshold) 
            + "-" + t_coder_offset::type() + "-" + t_coder_len::type();
    }

    void set_offset_prime(const sdsl::int_vector<32>& p) {
        m_offset_prime.resize(p.size());
        for(size_t i=0;i<p.size();i++) m_offset_prime[i] = p[i];
    }

    void set_length_prime(const sdsl::int_vector<32>& p) {
        m_length_prime.resize(p.size());
        for(size_t i=0;i<p.size();i++) m_length_prime[i] = p[i];
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs,block_factor_data& bfd) const
    {
        std::for_each(bfd.lengths.begin(),bfd.lengths.begin()+bfd.num_factors, [](uint32_t &n){ n--; });
        len_coder.set_deflate_dictionary((const uint8_t*)m_length_prime.data(),m_length_prime.size()*4);
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        offsetliteral_coder.set_deflate_dictionary((const uint8_t*)m_offset_prime.data(),m_offset_prime.size()*4);
        offsetliteral_coder.encode(ofs, bfd.offset_literals.data(), bfd.num_offset_literals);
    }

    template <class t_istream>
    void decode_block(t_istream& ifs, block_factor_data& bfd, size_t num_factors) const
    {
        bfd.num_factors = num_factors;
        len_coder.set_inflate_dictionary((const uint8_t*)m_length_prime.data(),m_length_prime.size()*4);
        len_coder.decode(ifs, bfd.lengths.data(), num_factors);
        offsetliteral_coder.set_inflate_dictionary((const uint8_t*)m_offset_prime.data(),m_offset_prime.size()*4);
        offsetliteral_coder.decode(ifs, bfd.offset_literals.data(), num_factors);
        std::for_each(bfd.lengths.begin(),bfd.lengths.begin()+num_factors, [](uint32_t &n){ n++; });
        bfd.num_literals = 0;
        bfd.num_offsets = 0;
        for(size_t i=0;i<bfd.num_factors;i++) {
            if(bfd.lengths[i] <= literal_threshold) {
                std::copy(bfd.offset_literals.begin()+i,bfd.offset_literals.begin()+i+bfd.lengths[i],bfd.literals.begin()+bfd.num_literals);
                bfd.num_literals += bfd.lengths[i];
            } else {
                bfd.offsets[bfd.num_offsets++] = bfd.offset_literals[i];
            }
        }
    }

    inline size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += m_offset_prime.serialize(out,child,"offset prime");
        written_bytes += m_length_prime.serialize(out,child,"length prime");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        m_offset_prime.load(in);
        m_length_prime.load(in);
    }

    size_type size_in_bytes() const {
        return sdsl::size_in_bytes(*this);
    }
};

