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
class t_coder_offset = coder::u32,
class t_coder_len = coder::vbyte
>
struct factor_coder {
	static std::string type() {
		return "factor_coder-"+t_coder_offset::type()+"-"+t_coder_len::type();
	}

	template<class t_ostream>
	static void encode_block(t_ostream& ofs,const std::vector<factor_data>& factors,size_t num_factors) {
		for(size_t i=0;i<num_factors;i++) {
			t_coder_offset::encode_check_size(ofs,factors[i].offset);
			t_coder_len::encode_check_size(ofs,factors[i].len);
		}
	}

	template<class t_istream>
	static void decode_block(t_istream& ifs,std::vector<factor_data>& factors,size_t num_factors) {
		for(size_t i=0;i<num_factors;i++) {
			auto& decoded_factor = factors[i];
			decoded_factor.offset = t_coder_offset::decode(ifs);
			decoded_factor.len = t_coder_len::decode(ifs);
		}
	}
};

/*
	encode factors in blocks.
 */
template <
class t_coder_offset = coder::u32,
class t_coder_len = coder::vbyte
>
struct factor_coder_blocked {
	static std::string type() {
		return "factor_coder_blocked-"+t_coder_offset::type()+"-"+t_coder_len::type();
	}

	template<class t_ostream>
	static void encode_block(t_ostream& ofs,const std::vector<factor_data>& factors,size_t num_factors) {
		for(size_t i=0;i<num_factors;i++)
			t_coder_offset::encode(ofs,factors[i].offset);
		for(size_t i=0;i<num_factors;i++)
			t_coder_offset::encode(ofs,factors[i].len);
	}

	template<class t_istream>
	static void decode_block(t_istream& ifs,std::vector<factor_data>& factors,size_t num_factors) {
		for(size_t i=0;i<num_factors;i++) {
			auto& decoded_factor = factors[i];
			decoded_factor.offset = t_coder_offset::decode(ifs);
		}
		for(size_t i=0;i<num_factors;i++) {
			auto& decoded_factor = factors[i];
			decoded_factor.len = t_coder_len::decode(ifs);
		}
	}
};

