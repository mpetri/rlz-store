#pragma once

#include <ratio>

#include "utils.hpp"
#include "collection.hpp"

struct dict_prune_none {
    static std::string type()
    {
        return "dict_prune_none";
    }

    template <class t_dict_idx, class t_factorization_strategy>
    static void prune(collection&, bool,
        uint64_t, uint64_t)
    {
        LOG(INFO) << "\t"
                  << "No dictionary pruning performed.";
    }
};
