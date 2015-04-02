#pragma once

#include "utils.hpp"
#include "collection.hpp"

struct factor_select_first {
	static std::string type() {
		return "factor_select_first";
	}

	template<class t_sa>
	static uint32_t pick_offset(const t_sa& sa,size_t sp,size_t,size_t factor_len,bool reverse_dict)
	{
		auto sa_val = sa[sp];
		if(reverse_dict) {
			return sa.size() - (sa_val+factor_len) - 1;
		} 
		return sa_val;
	}
};

struct factor_select_last {
	static std::string type() {
		return "factor_select_last";
	}

	template<class t_sa>
	static uint32_t pick_offset(const t_sa& sa,size_t,size_t ep,size_t factor_len,bool reverse_dict)
	{
		if(reverse_dict) {
			return sa.size() - (sa[ep]+factor_len) - 1;
		} 
		return sa[ep];
	}
};
