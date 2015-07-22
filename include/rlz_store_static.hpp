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

template <class t_dictionary_creation_strategy,
          class t_dictionary_pruning_strategy,
          class t_dictionary_index,
          uint32_t t_factorization_block_size,
          class t_factor_selection_strategy,
          class t_factor_coder,
          class t_block_map>
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

public:
    enum { block_size = t_factorization_block_size };
    uint64_t encoding_block_size = block_size;
    block_map_type& block_map = m_blockmap;
    sdsl::int_vector<8>& dict = m_dict;
    factor_coder_type m_factor_coder;
    sdsl::int_vector_mapper<1, std::ios_base::in>& factor_text = m_factored_text;
    uint64_t text_size;
    std::string m_dict_hash;
    std::string m_dict_file;
    std::string m_factor_file;
public:
    class builder;

    std::string type() const
    {
        auto dict_size_mb = dict.size() / (1024 * 1024);
        return dictionary_creation_strategy::type() + "-" + std::to_string(dict_size_mb) + "_"
               + dictionary_pruning_strategy::type() + "_"
               + factor_selection_strategy::type() + "_"
               + factor_coder_type::type();
    }

    rlz_store_static() = delete;
    rlz_store_static(rlz_store_static&&) = default;
    rlz_store_static& operator=(rlz_store_static&&) = default;
    rlz_store_static(collection& col)
        : m_factored_text(col.file_map[KEY_FACTORIZED_TEXT])
        , m_factor_stream(m_factored_text) // (1) mmap factored text
    {
        LOG(INFO) << "Loading RLZ store into memory";
        m_factor_file = col.file_map[KEY_FACTORIZED_TEXT];
        // (2) load the block map
        LOG(INFO) << "\tLoad block map";
        sdsl::load_from_file(m_blockmap, col.file_map[KEY_BLOCKMAP]);

        LOG(INFO) << "\tLoad factor coder";
        sdsl::load_from_file(m_factor_coder, col.file_map[KEY_FCODER]);

        // (3) load dictionary from disk
        LOG(INFO) << "\tLoad dictionary";
        m_dict_hash = col.param_map[PARAM_DICT_HASH];
        m_dict_file = col.file_map[KEY_DICT];
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

    size_type size_in_bytes() const {
        return m_dict.size() +
            (m_factored_text.size()>>3)+
            m_blockmap.size_in_bytes() +
            m_factor_coder.size_in_bytes();
    }

    inline void decode_factors(size_t offset,
                               block_factor_data& bfd,
                               size_t num_factors) const
    {
        m_factor_stream.seek(offset);
        m_factor_coder.decode_block(m_factor_stream, bfd, num_factors);
    }

    inline uint64_t decode_block(uint64_t block_id, std::vector<uint8_t>& text, block_factor_data& bfd) const
    {
        auto block_start = m_blockmap.block_offset(block_id);
        auto num_factors = m_blockmap.block_factors(block_id);
        decode_factors(block_start, bfd, num_factors);

        auto out_itr = text.begin();
        size_t literals_used = 0;
        size_t offsets_used = 0;
        for (size_t i = 0; i < num_factors; i++) {
            const auto& factor_len = bfd.lengths[i];
            if (factor_len <= m_factor_coder.literal_threshold) {
                /* copy literals */
                for (size_t i = 0; i < factor_len; i++) {
                    *out_itr = bfd.literals[literals_used + i];
                    out_itr++;
                }
                literals_used += factor_len;
            } else {
                /* copy from dict */
                const auto& factor_offset = bfd.offsets[offsets_used];
                auto begin = m_dict.begin() + factor_offset;
                std::copy(begin, begin + factor_len, out_itr);
                out_itr += factor_len;
                offsets_used++;
            }
        }
        auto written_syms = std::distance(text.begin(), out_itr);
        return written_syms;
    }

    std::vector<uint8_t>
    block(const size_t block_id) const
    {
        block_factor_data bfd(block_size);
        std::vector<uint8_t> block_content(block_size);
        auto decoded_syms = decode_block(block_id, block_content, bfd);
        block_content.resize(decoded_syms);
        return block_content;
    }


};
