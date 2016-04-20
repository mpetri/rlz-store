#pragma once

#include "utils.hpp"
#include "collection.hpp"

struct factor_select_first {
    static std::string type()
    {
        return "factor_select_first";
    }

    template <class t_index, class t_itr>
    static uint32_t pick_offset(const t_index& idx, const t_itr factor_itr, bool local_search, uint32_t block_size)
    {
        if (local_search) {
            if (factor_itr.local) {
                return factor_itr.local_offset;
            }
            if (idx.is_reverse()) {
                return block_size + (idx.sa.size() - (idx.sa[factor_itr.sp] + factor_itr.len) - 1);
            }
            return block_size + idx.sa[factor_itr.sp];
        }
        else {
            if (idx.is_reverse()) {
                return idx.sa.size() - (idx.sa[factor_itr.sp] + factor_itr.len) - 1;
            }
            return idx.sa[factor_itr.sp];
        }
    }
};

struct factor_select_last {
    static std::string type()
    {
        return "factor_select_last";
    }

    template <class t_index>
    static uint32_t pick_offset(const t_index& idx, size_t, size_t ep, size_t factor_len)
    {
        if (idx.is_reverse()) {
            return idx.sa.size() - (idx.sa[ep] + factor_len) - 1;
        }
        return idx.sa[ep];
    }
};

