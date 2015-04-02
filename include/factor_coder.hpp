#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "bit_coders.hpp"

#include <sdsl/suffix_arrays.hpp>

struct factor_data {
	uint32_t offset;
	uint32_t len;
};

template <
class t_coder_offset = coder::vbyte,
class t_coder_len = coder::vbyte
>
struct factor_coder {
	static std::string type() {
		return "factor_coder-"+t_coder_offset::type()+"-"+t_coder_len::type();
	}

	template<class t_ostream>
	static void encode_block(t_ostream& ofs,std::vector<factor_data>& factors,size_t num_factors) {
		for(size_t i=0;i<num_factors;i++) {
			t_coder_offset::encode_check_size(ofs,factors[i].offset);
			t_coder_len::encode_check_size(ofs,factors[i].len);
		}
	}

	template<class t_istream,class t_outitr>
	static void decode_block(t_istream& ifs,size_t num_factors,t_outitr out) {
		for(size_t i=0;i<num_factors;i++) {
			auto& decoded_factor = *out;
			decoded_factor.offset = t_coder_offset::decode(ifs);
			decoded_factor.len = t_coder_len::decode(ifs);
			++out;
		}
	}
};

/*
	encode factors in blocks.
 */
template <
class t_coder_offset = coder::vbyte,
class t_coder_len = coder::vbyte
>
struct factor_coder_blocked {
	static std::string type() {
		return "factor_coder_blocked-"+t_coder_offset::type()+"-"+t_coder_len::type();
	}

	template<class t_ostream>
	static void encode_block(t_ostream& ofs,std::vector<factor_data>& factors,size_t num_factors) {
		for(size_t i=0;i<num_factors;i++)
			t_coder_offset::encode(ofs,factors[i].offset);
		for(size_t i=0;i<num_factors;i++)
			t_coder_offset::encode(ofs,factors[i].len);
	}

	void decode() {

	}
};
