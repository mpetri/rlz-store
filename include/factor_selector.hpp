#pragma once

#include "utils.hpp"
#include "collection.hpp"

struct factor_select_first {
    static std::string type()
    {
        return "factor_select_first";
    }

    template <class t_index>
    static uint32_t pick_offset(const t_index& idx, size_t sp, size_t, size_t factor_len)
    {
        if (idx.is_reverse()) {
            return idx.sa.size() - (idx.sa[sp] + factor_len) - 1;
        }
        return idx.sa[sp];
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
