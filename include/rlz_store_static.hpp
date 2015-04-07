#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "block_maps.hpp"
#include "factor_selector.hpp"
#include "factor_strategies.hpp"
#include "factor_coder.hpp"
#include "dict_strategies.hpp"

#include <sdsl/suffix_arrays.hpp>

template <
class t_dictionary_creation_strategy = dict_random_sample_budget<1024,1024>,
class t_dictionary_pruning_strategy = dict_prune_none,
class t_dictionary_index = dict_index_csa<>,
class t_factorization_strategy = factor_strategy_greedy_backward_search<2048,factor_select_first>,
class t_factor_coder = factor_coder<coder::vbyte,coder::vbyte>,
class t_block_map = block_map_uncompressed
>
class rlz_store_static {
public:
    using dictionary_creation_strategy = t_dictionary_creation_strategy;
    using dictionary_pruning_strategy = t_dictionary_pruning_strategy;
    using dictionary_index = t_dictionary_index;
    using factorization_strategy = t_factorization_strategy;
    using factor_encoder = t_factor_coder;
    using block_map = t_block_map;
private:
    sdsl::int_vector_mapper<1,std::ios_base::in> m_factored_text;
    bit_istream<sdsl::int_vector_mapper<1,std::ios_base::in>> m_factor_stream;
    sdsl::int_vector<8>  m_dict;
    block_map m_blockmap;
public:
    static rlz_store_static create_or_load(collection& col) {
        using clock = std::chrono::high_resolution_clock;
        auto start = clock::now();
        // (1) create dictionary based on parametrized
        // dictionary creation strategy if necessary
        LOG(INFO) << "Create dictionary (" << dictionary_creation_strategy::type() << ")";
        dictionary_creation_strategy::create(col);
        LOG(INFO) << "Dictionary hash before pruning '" << col.param_map[PARAM_DICT_HASH] << "'";

        // (2) prune the dictionary if necessary
        LOG(INFO) << "Prune dictionary with " << dictionary_pruning_strategy::type();
        dictionary_pruning_strategy::template prune<factorization_strategy>(col);
        LOG(INFO) << "Dictionary after pruning '" << col.param_map[PARAM_DICT_HASH] << "'";

        // (3) create factorized text using the dict
        {
            LOG(INFO) << "Create/Load dictionary index";
            dictionary_index idx(col);

            LOG(INFO) << "Factorize text";
            const sdsl::int_vector_mapper<8,std::ios_base::in> text(col.file_map[KEY_TEXT]);
            auto encoding = factorizor::template encode<factor_encoder>(col,text.begin(),text.end());

            LOG(INFO) << "Merge factorized text blocks";
        }

        // (4) encode document start pos
        LOG(INFO) << "Create block map (" << block_map::type() << ")";
        auto blockmap_file = col.path+"/index/"+KEY_BLOCKMAP+"-"+factorization_strategy::type()+"-"+
            col.param_map[PARAM_DICT_HASH]+".sdsl";
        if(!utils::file_exists(blockmap_file)) {
            block_map tmp(col);
            sdsl::store_to_file(tmp,blockmap_file);
        }
        col.file_map[KEY_BLOCKMAP] = blockmap_file;

        auto stop = clock::now();
        LOG(INFO) << "RLZ construction complete. time = "
                  << std::chrono::duration_cast<std::chrono::seconds>(stop - start).count()
                  << " sec";
        return rlz_store_static(col);
    }
    rlz_store_static() = delete;
    rlz_store_static(rlz_store_static&&) = default;
    rlz_store_static& operator=(rlz_store_static&&) = default;
    rlz_store_static(collection& col) 
    : m_factored_text(col.file_map[KEY_FACTORIZED_TEXT]), m_factor_stream(m_factored_text) // (1) mmap factored text
    {
        // (2) load the block map
        LOG(INFO) << "Load block map from " << col.file_map[KEY_BLOCKMAP];
        sdsl::load_from_file(m_blockmap,col.file_map[KEY_BLOCKMAP]);

        // (3) load dictionary from disk
        LOG(INFO) << "Load dictionary from " << col.file_map[KEY_DICT];
        sdsl::load_from_file(m_dict,col.file_map[KEY_DICT]);
        LOG(INFO) << "RLZ store ready";
    }

    std::vector<uint8_t>
    block(const size_t block_id) const {
        std::vector<uint8_t> block_content;
        auto block_start = m_blockmap.block_offset(block_id);
        auto num_factors = m_blockmap.block_factors(block_id);
        m_factor_stream.seek(block_start);

        LOG(TRACE) << "block id = " << block_id;
        LOG(TRACE) << "block start = " << block_start;
        LOG(TRACE) << "num factors = " << num_factors;

        std::vector<factor_data> factors(num_factors);
        factor_encoder::decode_block(m_factor_stream,num_factors,factors.begin());

        size_t i=0;
        for(const auto& factor : factors) {
            LOG(TRACE) << "DECODE FACTOR(" << i << "," << factor.offset << "," << factor.len << ")";
            if(factor.len!=0) {
                auto begin = m_dict.begin()+factor.offset;
                std::copy(begin,begin+factor.len,std::back_inserter<>(block_content));
            } else {
                block_content.push_back(factor.offset);
            }
            i++;
        }

        return block_content;
    }

    void
    block(const size_t block_id,std::vector<uint8_t>& block_content) const {
        auto block_start = m_blockmap.block_offset(block_id);
        auto num_factors = m_blockmap.block_factors(block_id);
        m_factor_stream.seek(block_start);

        std::vector<factor_data> factors(num_factors);
        factor_encoder::decode_block(m_factor_stream,num_factors,factors.begin());

        size_t cur = 0;
        for(const auto& factor : factors) {
            if(factor.len!=0) {
                auto begin = m_dict.begin()+factor.offset;
                std::copy(begin,begin+factor.len,block_content.begin()+cur);
                cur += factor.len;
            } else {
                block_content[cur] = (uint8_t) factor.offset;
                cur++;
            }
        }

        return block_content;
    }
};
