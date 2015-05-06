#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "rlz_store_static.hpp"

template <class t_dictionary_creation_strategy,
          class t_dictionary_pruning_strategy,
          class t_dictionary_index,
          uint32_t t_factorization_block_size,
          class t_factor_selection_strategy,
          class t_factor_coder,
          class t_block_map>
class rlz_store_static<t_dictionary_creation_strategy,
                       t_dictionary_pruning_strategy,
                       t_dictionary_index,
                       t_factorization_block_size,
                       t_factor_selection_strategy,
                       t_factor_coder,
                       t_block_map>::builder {
public:
    using dictionary_creation_strategy = t_dictionary_creation_strategy;
    using dictionary_pruning_strategy = t_dictionary_pruning_strategy;
    using dictionary_index = t_dictionary_index;
    using factor_selection_strategy = t_factor_selection_strategy;
    using factor_encoder = t_factor_coder;
    using factorization_strategy = factorizor<t_factorization_block_size, dictionary_index, factor_selection_strategy, factor_encoder>;
    using block_map = t_block_map;

public:
    builder& set_rebuild(bool r)
    {
        rebuild = r;
        return *this;
    };
    builder& set_threads(uint8_t nt)
    {
        num_threads = nt;
        return *this;
    };
    builder& set_dict_size(uint64_t ds)
    {
        dict_size_bytes = ds;
        return *this;
    };

    static std::string blockmap_file_name(collection& col)
    {
        return col.path + "/index/" + KEY_BLOCKMAP + "-"
               + block_map_type::type() + "-" + factorization_strategy::type() + "-"
               + col.param_map[PARAM_DICT_HASH] + ".sdsl";
    }

    rlz_store_static build_or_load(collection& col) const
    {
        auto start = hrclock::now();

        // (1) create dictionary based on parametrized
        // dictionary creation strategy if necessary
        LOG(INFO) << "Create dictionary (" << dictionary_creation_strategy::type() << ")";
        dictionary_creation_strategy::create(col, rebuild, dict_size_bytes);
        LOG(INFO) << "Dictionary hash before pruning '" << col.param_map[PARAM_DICT_HASH] << "'";

        // (2) prune the dictionary if necessary
        LOG(INFO) << "Prune dictionary with " << dictionary_pruning_strategy::type();
        dictionary_pruning_strategy::template prune<factorization_strategy>(col, rebuild);
        LOG(INFO) << "Dictionary after pruning '" << col.param_map[PARAM_DICT_HASH] << "'";

        // (3) create factorized text using the dict
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        auto factor_file_name = factorization_strategy::factor_file_name(col);
        if (rebuild || !utils::file_exists(factor_file_name)) {
            LOG(INFO) << "Create/Load dictionary index";
            dictionary_index idx(col, rebuild);

            LOG(INFO) << "Factorize text (" << num_threads << " threads) - ("
                      << factorization_strategy::type() << ")";
            auto start_fact = hrclock::now();
            const sdsl::int_vector_mapper<8, std::ios_base::in> text(col.file_map[KEY_TEXT]);

            std::vector<std::future<factorization_info> > fis;
            std::vector<factorization_info> efs;
            auto num_blocks = text.size() / t_factorization_block_size;
            auto blocks_per_thread = num_blocks / num_threads;
            auto syms_per_thread = blocks_per_thread * t_factorization_block_size;
            auto left = text.size();
            auto itr = text.begin();
            for (size_t i = 0; i < num_threads; i++) {
                auto begin = itr;
                auto end = begin + syms_per_thread;
                if (left < 2 * syms_per_thread) // last thread might have to encode a little more
                    end = text.end();
                fis.push_back(std::async(std::launch::async, [&, begin, end, i] {
                        return factorization_strategy::factorize(col,idx,begin,end,i);
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
            auto text_size_mb = text.size() / (1024 * 1024.0);
            auto fact_seconds = duration_cast<milliseconds>(stop_fact - start_fact).count() / 1000.0;
            LOG(INFO) << "Factorization time = " << fact_seconds << " sec";
            LOG(INFO) << "Factorization speed = " << text_size_mb / fact_seconds << " MB/s";

            output_encoding_stats(efs, text.size());

            LOG(INFO) << "Merge factorized text blocks";
            merge_factor_encodings<factorization_strategy>(col, efs);
        } else {
            LOG(INFO) << "Factorized text exists.";
            col.file_map[KEY_FACTORIZED_TEXT] = factor_file_name;
            col.file_map[KEY_BLOCKOFFSETS] = factorization_strategy::boffsets_file_name(col);
            col.file_map[KEY_BLOCKFACTORS] = factorization_strategy::bfactors_file_name(col);
        }

        // (4) encode document start pos
        LOG(INFO) << "Create block map (" << block_map_type::type() << ")";
        auto blockmap_file = blockmap_file_name(col);
        if (rebuild || !utils::file_exists(blockmap_file)) {
            block_map_type tmp(col);
            sdsl::store_to_file(tmp, blockmap_file);
        }
        col.file_map[KEY_BLOCKMAP] = blockmap_file;

        auto stop = hrclock::now();
        LOG(INFO) << "RLZ construction complete. time = " << duration_cast<seconds>(stop - start).count() << " sec";

        return rlz_store_static(col);
    }

    rlz_store_static load(collection& col) const
    {
        /* make sure components exists and register them */

        /* (1) check dict */
        auto dict_file_name = dictionary_creation_strategy::file_name(col, dict_size_bytes);
        if (!utils::file_exists(dict_file_name)) {
            throw std::runtime_error("LOAD FAILED: Cannot find dictionary.");
        } else {
            col.file_map[KEY_DICT] = dict_file_name;
            col.compute_dict_hash();
        }

        /* (2) check factorized text */
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        auto factor_file_name = factorization_strategy::factor_file_name(col);
        if (!utils::file_exists(factor_file_name)) {
            throw std::runtime_error("LOAD FAILED: Cannot find factorized text.");
        } else {
            col.file_map[KEY_FACTORIZED_TEXT] = factor_file_name;
            col.file_map[KEY_BLOCKOFFSETS] = factorization_strategy::boffsets_file_name(col);
            col.file_map[KEY_BLOCKFACTORS] = factorization_strategy::bfactors_file_name(col);
        }

        /* (2) check blockmap */
        auto blockmap_file = blockmap_file_name(col);
        if (!utils::file_exists(blockmap_file)) {
            throw std::runtime_error("LOAD FAILED: Cannot find blockmap.");
        } else {
            col.file_map[KEY_BLOCKMAP] = blockmap_file;
        }

        /* load */
        return rlz_store_static(col);
    }

    template <class t_idx>
    rlz_store_static reencode(collection& col, t_idx& old) const
    {
        /* (0) make sure we use the same dict, and coding strategy etc. */
        static_assert(std::is_same<typename t_idx::dictionary_creation_strategy, dictionary_creation_strategy>::value,
                      "different dictionary creation strategy");
        static_assert(std::is_same<typename t_idx::dictionary_pruning_strategy, dictionary_pruning_strategy>::value,
                      "different dictionary pruning strategy");
        static_assert(std::is_same<typename t_idx::dictionary_index, dictionary_index>::value,
                      "different dictionary index");
        static_assert(std::is_same<typename t_idx::factor_selection_strategy, factor_selection_strategy>::value,
                      "different factor selection strategy");
        static_assert(t_idx::factorization_bs == t_factorization_block_size,
                      "different factorization block size");
        static_assert(t_idx::factor_coder_type::literal_threshold == factor_coder_type::literal_threshold,
                      "different factor literal coding threshold");

        /* (1) check dict */
        auto dict_file_name = dictionary_creation_strategy::file_name(col, dict_size_bytes);
        if (!utils::file_exists(dict_file_name)) {
            throw std::runtime_error("Cannot find dictionary.");
        } else {
            col.file_map[KEY_DICT] = dict_file_name;
            col.compute_dict_hash();
        }

        /* (2) reencode the factors */
        auto start = hrclock::now();
        LOG(INFO) << "Reencoding factors (" << t_factor_coder::type() << ")";
        auto itr = old.factors_begin();
        auto end = old.factors_end();
        block_factor_data bfd;
        auto factor_file_name = factorization_strategy::factor_file_name(col);
        auto boffsets_file_name = factorization_strategy::boffsets_file_name(col);
        auto bfactors_file_name = factorization_strategy::bfactors_file_name(col);
        if (rebuild || !utils::file_exists(factor_file_name)) {
            auto factor_buf = sdsl::write_out_buffer<1>::create(factor_file_name);
            auto block_offsets = sdsl::write_out_buffer<0>::create(boffsets_file_name);
            auto block_factors = sdsl::write_out_buffer<0>::create(bfactors_file_name);
            bit_ostream<sdsl::int_vector_mapper<1> > factor_stream(factor_buf);
            size_t cur_block_offset = itr.block_id;
            t_factor_coder coder;
            auto num_blocks = old.block_map.num_blocks();
            auto num_blocks10p = (uint64_t)(num_blocks * 0.1);

            while (itr != end) {
                const auto& f = *itr;
                if (itr.block_id != cur_block_offset) {
                    block_offsets.push_back(factor_stream.tellp());
                    block_factors.push_back(bfd.num_factors);
                    coder.encode_block(factor_stream,bfd);
                    cur_block_offset = itr.block_id;
                    bfd.reset();
                    if ((cur_block_offset + 1) % num_blocks10p == 0) {
                        LOG(INFO) << "\t"
                                  << "Encoded " << 100 * (cur_block_offset + 1) / num_blocks << "% ("
                                  << (cur_block_offset + 1) << "/" << num_blocks << ")";
                    }
                }

                if(f.is_literal) {
                    bfd.add_factor(coder,f.literal_ptr,0,f.len);
                } else {
                    bfd.add_factor(coder,f.literal_ptr,f.offset,f.len);
                }
                ++itr;
            }
            if ( bfd.num_factors != 0) {
                block_offsets.push_back(factor_stream.tellp());
                block_factors.push_back(bfd.num_factors);
                coder.encode_block(factor_stream,bfd);
            }
        }
        col.file_map[KEY_FACTORIZED_TEXT] = factor_file_name;
        col.file_map[KEY_BLOCKOFFSETS] = boffsets_file_name;
        col.file_map[KEY_BLOCKFACTORS] = bfactors_file_name;

        LOG(INFO) << "\t"
                  << "Create block map (" << block_map_type::type() << ")";
        auto blockmap_file = blockmap_file_name(col);
        if (rebuild || !utils::file_exists(blockmap_file)) {
            block_map_type tmp(col);
            sdsl::store_to_file(tmp, blockmap_file);
        }
        col.file_map[KEY_BLOCKMAP] = blockmap_file;
        auto stop = hrclock::now();
        LOG(INFO) << "RLZ factor reencode complete. time = " << duration_cast<seconds>(stop - start).count() << " sec";
        return rlz_store_static(col);
    }

private:
    bool rebuild = false;
    uint32_t num_threads = 1;
    uint64_t dict_size_bytes = 0;
};
