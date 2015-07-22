#pragma once

#include "utils.hpp"
#include "collection.hpp"

class dict_none {
public:
    static std::string type()
    {
        return "dict_none";
    }

    static std::string file_name(collection& col)
    {
        return col.path + "/index/" + type() + ".sdsl";
    }

public:
    static void create(collection& col, bool , size_t )
    {
        auto fname = file_name(col);
        col.file_map[KEY_DICT] = fname;
        auto wdict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
        wdict.push_back(0);
        col.compute_dict_hash();
    }

    static uint64_t compute_closest_dict_offset(size_t ,size_t ,size_t ,size_t )
    {
        return 0;
    }
};
