#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "factor_coder.hpp"
#include "bit_streams.hpp"
#include "factor_storage.hpp"

#include <sdsl/suffix_arrays.hpp>
#include <sdsl/int_vector_mapper.hpp>

#include <cctype>

template <uint32_t t_block_size,
          class t_index,
          class t_factor_selector,
          class t_coder>
struct factorizor {
    static std::string type()
    {
        return "factorizor-" + std::to_string(t_block_size) + "-" + t_factor_selector::type() + "-" + t_coder::type();
    }

    static std::string factor_file_name(collection& col)
    {
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        return col.path + "/index/" + type() + "-dhash=" + dict_hash + ".sdsl";
    }

    static std::string boffsets_file_name(collection& col)
    {
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        return col.path + "/index/" + KEY_BLOCKOFFSETS + "-fs=" + type() + "-dhash=" + dict_hash + ".sdsl";
    }

    static std::string bfactors_file_name(collection& col)
    {
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        return col.path + "/index/" + KEY_BLOCKFACTORS + "-fs=" + type() + "-dhash=" + dict_hash + ".sdsl";
    }

    template <class t_fstorage,class t_itr>
    static void factorize_block(t_fstorage& fs, t_coder& coder, const t_index& idx, t_itr itr, t_itr end)
    {
        auto factor_itr = idx.factorize(itr, end);
        fs.start_new_block();
        size_t syms_encoded = 0;
        while (!factor_itr.finished()) {
            if (factor_itr.len == 0) {
                fs.add_to_block_factor(coder, itr + syms_encoded, 0, 1);
                syms_encoded++;
            } else {
                auto offset = t_factor_selector::template pick_offset<>(idx, factor_itr.sp, factor_itr.ep, factor_itr.len);
                fs.add_to_block_factor(coder, itr + syms_encoded, offset, factor_itr.len);
                syms_encoded += factor_itr.len;
            }

            ++factor_itr;
        }
        fs.encode_current_block(coder);
    }

    template <class t_itr>
    static factorization_info
    factorize(collection& col, t_index& idx, t_itr itr, t_itr end, size_t offset = 0)
    {
        /* (1) create output files */
        factor_storage fs(col, t_block_size, offset);

        /* (2) create encoder and load priming data from file if necessary */
        t_coder coder;
        sdsl::load_from_file(coder, col.file_map[KEY_FCODER]);

        /* (3) compute text stats */
        auto block_size = t_block_size;
        size_t n = std::distance(itr, end);
        size_t num_blocks = n / block_size;
        auto left = n % block_size;
        auto blocks_per_10mib = (10 * 1024 * 1024) / block_size;

        /* (4) encode blocks */
        for (size_t i = 1; i <= num_blocks; i++) {
            auto block_end = itr + block_size;
            // LOG(INFO) << "block " << i;
            factorize_block(fs, coder, idx, itr, block_end);
            itr = block_end;
            block_end += block_size;
            if (i % blocks_per_10mib == 0) {
                fs.output_stats(num_blocks);
            }
        }

        /* (5) is there a non-full block? */
        if (left != 0) {
            factorize_block(fs, coder, idx, itr, end);
        }

        return fs.info();
    }

    template <class t_itr>
    static factorization_statistics
    factorize_statistics(collection& col, t_index& idx, t_itr itr, t_itr end)
    {
        /* (1) create output files */
        factor_tracker ft(col, t_block_size);

        /* (2) create encoder and load priming data from file if necessary */
        t_coder coder;
        sdsl::load_from_file(coder, col.file_map[KEY_FCODER]);

        /* (3) compute text stats */
        auto block_size = t_block_size;
        size_t n = std::distance(itr, end);
        size_t num_blocks = n / block_size;
        auto left = n % block_size;
        auto blocks_per_10mib = (10 * 1024 * 1024) / block_size;

        /* (4) encode blocks */
        for (size_t i = 1; i <= num_blocks; i++) {
            auto block_end = itr + block_size;
            // LOG(INFO) << "block " << i;
            factorize_block(ft, coder, idx, itr, block_end);
            itr = block_end;
            block_end += block_size;
            if (i % blocks_per_10mib == 0) {
                ft.output_stats(num_blocks);
            }
        }

        /* (5) is there a non-full block? */
        if (left != 0) {
            factorize_block(ft, coder, idx, itr, end);
        }

        return ft.statistics();
    }
};
