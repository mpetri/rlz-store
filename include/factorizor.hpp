#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "factor_coder.hpp"
#include "bit_streams.hpp"
#include "factor_storage.hpp"

#include <sdsl/suffix_arrays.hpp>
#include <sdsl/int_vector_mapped_buffer.hpp>

#include <cctype>
#include <future>

template <uint32_t t_block_size,
          bool t_search_local_block_context,
          class t_index,
          class t_factor_selector,
          class t_coder>
struct factorizor {
    static std::string type()
    {
        return "factorizor-" + std::to_string(t_block_size) + "-l" + std::to_string(t_search_local_block_context) + "-" + t_factor_selector::type() + "-" + t_coder::type();
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

    template <class t_factor_store, class t_itr>
    static void factorize_block(t_factor_store& fs, t_coder& coder, const t_index& idx, t_itr itr, t_itr end)
    {
        // utils::rlz_timer<std::chrono::milliseconds> fbt("factorize_block");
        uint64_t encoding_block_size = std::distance(itr,end);
        auto factor_itr = idx.factorize<decltype(itr),t_search_local_block_context>(itr, end);
        fs.start_new_block();
        size_t syms_encoded = 0;
        double local = 0;
        double global = 0;
        double factors = 0;
        while (!factor_itr.finished()) {
            if (factor_itr.len == 0) {
                fs.add_to_block_factor(coder, itr + syms_encoded, 0, 1);
                syms_encoded++;
            } else {
                auto offset = t_factor_selector::template pick_offset<>(idx, factor_itr,t_search_local_block_context,encoding_block_size);
                if(offset < encoding_block_size) {
                    local++;
                    // LOG(INFO) << "L <" << offset << "," << factor_itr.len << ">";
                } else {
                    global++;
                    // LOG(INFO) << "G <" << offset << "," << factor_itr.len << ">";
                }
                fs.add_to_block_factor(coder, itr + syms_encoded, offset, factor_itr.len);
                syms_encoded += factor_itr.len;
            }
            factors++;
            ++factor_itr;
        }
        //LOG(INFO) << "local = " << local << " global = " << global << " local / (local+global) = " << 100.0*local/factors;
        fs.encode_current_block(coder);
    }

    template <class t_factor_store, class t_itr>
    static typename t_factor_store::result_type
    factorize(collection& col, t_index& idx, t_itr _itr, t_itr _end, size_t offset = 0)
    {
        const sdsl::int_vector_mapped_buffer<8> text(col.file_map[KEY_TEXT]);
        auto itr = text.begin() + _itr;
        auto end = text.begin() + _end;

        /* (1) create output files */
        t_factor_store fs(col, t_block_size, offset);

        /* (2) create encoder  */
        t_coder coder;

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

        return fs.result();
    }

    static std::string factorcoder_file_name(collection& col)
    {
        return col.path + "/index/" + KEY_FCODER + "-"
               + t_coder::type() + "-" + type() + "-"
               + col.param_map[PARAM_DICT_HASH] + ".sdsl";
    }

    template <class t_factor_store>
    static typename t_factor_store::result_type
    parallel_factorize(collection& col, bool rebuild, uint32_t num_threads)
    {
        LOG(INFO) << "Create/Load dictionary index";
        t_index idx(col, rebuild);
        std::vector<typename t_factor_store::result_type> efs;
        {
            auto text_size = 0ULL;
            {
                const sdsl::int_vector_mapped_buffer<8> text(col.file_map[KEY_TEXT]);
                text_size = text.size();
            }
            auto text_size_mb = text_size / (1024 * 1024.0);
            auto start_fact = hrclock::now();
            LOG(INFO) << "Factorize text - " << text_size_mb << " MiB (" << num_threads << " threads) - (" << type() << ")";

            std::vector<std::future<typename t_factor_store::result_type> > fis;

            auto num_blocks = text_size / t_block_size;
            auto blocks_per_thread = num_blocks / num_threads;
            auto syms_per_thread = blocks_per_thread * t_block_size;
            auto left = text_size;
            auto itr = 0ULL;
            for (size_t i = 0; i < num_threads; i++) {
                auto begin = itr;
                auto end = begin + syms_per_thread;
                if (left < 2 * syms_per_thread) // last thread might have to encode a little more
                    end = text_size;
                fis.push_back(std::async(std::launch::async, [&, begin, end, i] {
                        return factorize<t_factor_store>(col,idx,begin,end,i);
                }));
                itr += syms_per_thread;
                left -= syms_per_thread;
            }
            // wait for all threads to finish
            for (auto& fi : fis) {
                auto f = fi.get();
                efs.push_back(f);
            }

            auto stop_fact = hrclock::now();
            auto fact_seconds = duration_cast<milliseconds>(stop_fact - start_fact).count() / 1000.0;
            LOG(INFO) << "Factorization time = " << fact_seconds << " sec";
            LOG(INFO) << "Factorization speed = " << text_size_mb / fact_seconds << " MB/s";
            LOG(INFO) << "Factorize done. (" << type() << ")";
        }
        LOG(INFO) << "Output factorization statistics";
        output_encoding_stats(col, efs);

        LOG(INFO) << "Merge factorized text blocks";
        return merge_factor_encodings<factorizor<t_block_size,t_search_local_block_context, t_index, t_factor_selector, t_coder> >(col, efs);
    }
};
