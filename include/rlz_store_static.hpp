#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "iterators.hpp"
#include "block_maps.hpp"
#include "factor_selector.hpp"
#include "factorizor.hpp"
#include "factor_coder.hpp"
#include "dict_strategies.hpp"
#include "dict_indexes.hpp"

#include <sdsl/suffix_arrays.hpp>

#include <future>

using namespace std::chrono;

template <class t_dictionary_creation_strategy = dict_random_sample_budget<1024, 1024>,
          class t_dictionary_pruning_strategy = dict_prune_none,
          class t_dictionary_index = dict_index_csa<>,
          uint32_t t_factorization_block_size = 2048,
          class t_factor_selection_strategy = factor_select_first,
          class t_factor_coder = factor_coder<coder::vbyte, coder::vbyte>,
          class t_block_map = block_map_uncompressed>
class rlz_store_static {
public:
    using dictionary_creation_strategy = t_dictionary_creation_strategy;
    using dictionary_pruning_strategy = t_dictionary_pruning_strategy;
    using dictionary_index = t_dictionary_index;
    using factor_selection_strategy = t_factor_selection_strategy;
    using factor_coder_type = t_factor_coder;
    using factorization_strategy = factorizor<t_factorization_block_size, dictionary_index, factor_selection_strategy, factor_coder_type>;
    using block_map_type = t_block_map;
    using size_type = uint64_t;

private:
    sdsl::int_vector_mapper<1, std::ios_base::in> m_factored_text;
    bit_istream<sdsl::int_vector_mapper<1, std::ios_base::in> > m_factor_stream;
    sdsl::int_vector<8> m_dict;
    block_map_type m_blockmap;
    uint64_t text_size;

public:
    uint32_t factorization_block_size = t_factorization_block_size;
    block_map_type& block_map = m_blockmap;
    sdsl::int_vector<8>& dict = m_dict;
    factor_coder_type factor_coder;

public:
    class builder;

    rlz_store_static() = delete;
    rlz_store_static(rlz_store_static&&) = default;
    rlz_store_static& operator=(rlz_store_static&&) = default;
    rlz_store_static(collection& col)
        : m_factored_text(col.file_map[KEY_FACTORIZED_TEXT])
        , m_factor_stream(m_factored_text) // (1) mmap factored text
    {
        // (2) load the block map
        LOG(INFO) << "\tLoad block map";
        sdsl::load_from_file(m_blockmap, col.file_map[KEY_BLOCKMAP]);
        // (3) load dictionary from disk
        LOG(INFO) << "\tLoad dictionary";
        sdsl::load_from_file(m_dict, col.file_map[KEY_DICT]);
        {
            LOG(INFO) << "\tDetermine text size";
            const sdsl::int_vector_mapper<8, std::ios_base::in> text(col.file_map[KEY_TEXT]);
            text_size = text.size();
        }
        LOG(INFO) << "RLZ store ready";
    }

    auto factors_begin() const -> factor_iterator<decltype(*this)>
    {
        return factor_iterator<decltype(*this)>(*this, 0, 0);
    }

    auto factors_end() const -> factor_iterator<decltype(*this)>
    {
        return factor_iterator<decltype(*this)>(*this, m_blockmap.num_blocks(), 0);
    }

    auto begin() const -> text_iterator<decltype(*this)>
    {
        return text_iterator<decltype(*this)>(*this, 0);
    }

    auto end() const -> text_iterator<decltype(*this)>
    {
        return text_iterator<decltype(*this)>(*this, size());
    }

    size_type size() const
    {
        return text_size;
    }

    void decode_factors(size_t offset,
                        std::vector<uint32_t>& offsets,
                        std::vector<uint32_t>& lens,
                        size_t num_factors) const
    {
        m_factor_stream.seek(offset);
        factor_coder.decode_block(m_factor_stream, offsets, lens, num_factors);
    }

    uint64_t decode_block(
        uint64_t block_id,
        std::vector<uint8_t>& text,
        std::vector<uint32_t>& offsets,
        std::vector<uint32_t>& lens) const
    {
        auto block_start = m_blockmap.block_offset(block_id);
        auto num_factors = m_blockmap.block_factors(block_id);
        decode_factors(block_start, offsets, lens, num_factors);

        auto out_itr = text.begin();
        for (size_t i = 0; i < num_factors; i++) {
            const auto& factor_len = lens[i];
            const auto& factor_offset = offsets[i];
            if (factor_len != 0) {
                auto begin = m_dict.begin() + factor_offset;
                std::copy(begin, begin + factor_len, out_itr);
                out_itr += factor_len;
            } else {
                *out_itr = factor_offset;
                out_itr++;
            }
        }
        auto written_syms = std::distance(text.begin(), out_itr);
        return written_syms;
    }

    std::vector<uint8_t>
    block(const size_t block_id) const
    {
        std::vector<uint8_t> block_content(factorization_block_size);
        std::vector<uint32_t> offsets(factorization_block_size);
        std::vector<uint32_t> lens(factorization_block_size);
        auto decoded_syms = decode_block(block_id, block_content, offsets, lens);
        block_content.resize(decoded_syms);
        return block_content;
    }
};

template <class t_dictionary_creation_strategy = dict_random_sample_budget<1024, 1024>,
          class t_dictionary_pruning_strategy = dict_prune_none,
          class t_dictionary_index = dict_index_csa<>,
          uint32_t t_factorization_block_size = 2048,
          class t_factor_selection_strategy = factor_select_first,
          class t_factor_coder = factor_coder<coder::vbyte, coder::vbyte>,
          class t_block_map = block_map_uncompressed>
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

    rlz_store_static build_or_load(collection& col) const
    {
        auto start = hrclock::now();

        // (1) create dictionary based on parametrized
        // dictionary creation strategy if necessary
        LOG(INFO) << "Create dictionary (" << dictionary_creation_strategy::type() << ")";
        dictionary_creation_strategy::create(col, rebuild);
        LOG(INFO) << "Dictionary hash before pruning '" << col.param_map[PARAM_DICT_HASH] << "'";

        // (2) prune the dictionary if necessary
        LOG(INFO) << "Prune dictionary with " << dictionary_pruning_strategy::type();
        dictionary_pruning_strategy::template prune<factorization_strategy>(col, rebuild);
        LOG(INFO) << "Dictionary after pruning '" << col.param_map[PARAM_DICT_HASH] << "'";

        // (3) create factorized text using the dict
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        auto factor_file_name = col.path + "/index/" + factorization_strategy::type() + "-dhash=" + dict_hash + ".sdsl";
        if (rebuild || !utils::file_exists(factor_file_name)) {
            LOG(INFO) << "Create/Load dictionary index";
            dictionary_index idx(col, rebuild);

            LOG(INFO) << "Factorize text (" << num_threads << " threads)";
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
            merge_factor_encodings(col, efs, factorization_strategy::type());
        } else {
            LOG(INFO) << "Factorized text exists.";
            auto boffsets_file_name = col.path + "/index/" + KEY_BLOCKOFFSETS + "-dhash=" + dict_hash + ".sdsl";
            auto bfactors_file_name = col.path + "/index/" + KEY_BLOCKFACTORS + "-dhash=" + dict_hash + ".sdsl";
            col.file_map[KEY_FACTORIZED_TEXT] = factor_file_name;
            col.file_map[KEY_BLOCKOFFSETS] = boffsets_file_name;
            col.file_map[KEY_BLOCKFACTORS] = bfactors_file_name;
        }

        // (4) encode document start pos
        LOG(INFO) << "Create block map (" << block_map_type::type() << ")";
        auto blockmap_file = col.path + "/index/" + KEY_BLOCKMAP + "-"
                             + block_map_type::type() + "-" + factorization_strategy::type() + "-"
                             + col.param_map[PARAM_DICT_HASH] + ".sdsl";
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
        auto dict_file_name = col.path + "/index/" + dictionary_creation_strategy::type() + ".sdsl";
        if (!utils::file_exists(dict_file_name)) {
            throw std::runtime_error("LOAD FAILED: Cannot find dictionary.");
        } else {
            col.file_map[KEY_DICT] = dict_file_name;
            col.compute_dict_hash();
        }

        /* (2) check factorized text */
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        auto factor_file_name = col.path + "/index/" + factorization_strategy::type() + "-dhash=" + dict_hash + ".sdsl";
        if (!utils::file_exists(factor_file_name)) {
            throw std::runtime_error("LOAD FAILED: Cannot find factorized text.");
        } else {
            auto boffsets_file_name = col.path + "/index/" + KEY_BLOCKOFFSETS + "-dhash=" + dict_hash + ".sdsl";
            auto bfactors_file_name = col.path + "/index/" + KEY_BLOCKFACTORS + "-dhash=" + dict_hash + ".sdsl";
            col.file_map[KEY_FACTORIZED_TEXT] = factor_file_name;
        }

        /* (2) check blockmap */
        auto blockmap_file = col.path + "/index/" + KEY_BLOCKMAP + "-"
                             + block_map_type::type() + "-" + factorization_strategy::type() + "-"
                             + col.param_map[PARAM_DICT_HASH] + ".sdsl";
        if (!utils::file_exists(blockmap_file)) {
            throw std::runtime_error("LOAD FAILED: Cannot find blockmap.");
        } else {
            col.file_map[KEY_BLOCKMAP] = blockmap_file;
        }

        /* load */
        return rlz_store_static(col);
    }

private:
    bool rebuild = false;
    uint32_t num_threads = 1;
};
