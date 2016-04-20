#pragma once

#include <sdsl/int_vector.hpp>
#include <string>
#include <sdsl/rmq_support.hpp>
#include "utils.hpp"

template <class t_csa, class t_itr, bool t_local_search>
struct factor_itr_csa {
    const t_csa& sa;
    t_itr factor_start;
    t_itr itr;
    t_itr start;
    t_itr end;
    uint64_t block_len;
    uint64_t sp;
    uint64_t ep;
    uint64_t len;
    uint64_t local_offset;
    uint8_t sym;
    bool done;
    bool local;
    std::unordered_map<uint64_t,utils::qgram_postings>& qgram_cache;

    factor_itr_csa(const t_csa& _csa, t_itr begin, t_itr _end,std::unordered_map<uint64_t,utils::qgram_postings>& qgc)
        : sa(_csa)
        , factor_start(begin)
        , itr(begin)
        , start(begin)
        , end(_end)
        , sp(0)
        , ep(_csa.size() - 1)
        , len(0)
        , local_offset(0)
        , sym(0)
        , done(false)
        , local(false)
        , qgram_cache(qgc)
    {
        qgram_cache.clear();
        find_next_factor();
    }
    factor_itr_csa& operator++()
    {
        find_next_factor();
        return *this;
    }
    
    inline uint64_t form_qgram(t_itr itr) {
        uint64_t qgram = 0;
        uint8_t* q8g = (uint8_t*) &qgram;
        q8g[0] = *itr++;
        q8g[1] = *itr++;
        q8g[2] = *itr++;
        q8g[3] = *itr++;
        q8g[4] = *itr++;
        q8g[5] = *itr++;
        q8g[6] = *itr++;
        q8g[7] = *itr;
        return qgram;
    } 
    
    inline void update_qgram(uint64_t& qgram,t_itr itr) {
        uint64_t sym = *itr;
        qgram = (qgram << 8) | sym;
    }
    
    inline void add_to_qgram_list(uint64_t qgram,size_t offset) {
        auto itr = qgram_cache.find(qgram);
        if(itr == qgram_cache.end()) {
            qgram_cache.emplace(qgram,utils::qgram_postings(offset));
        } else {
            auto& qgram_list = itr->second;
            qgram_list.last = qgram_list.last & (utils::max_pos_len-1);
            qgram_list.pos[qgram_list.last++] = offset;
            qgram_list.items++;
        }
    }

    inline void find_longer_local_factor()
    {
        local = false;
        // (1) local search enabled?
        if (!t_local_search)
            return;
        // (2) is the global factor already quite long? are we in the first part of the block?
        if(len >= 20 || (uint32_t) std::distance(factor_start,end) <= sizeof(uint64_t) ) return;

        // // (3) search for a better factor locally
        {
            // utils::rlz_timer<std::chrono::milliseconds> fbt("search q-gram");
            uint64_t qgram = form_qgram(factor_start);
            auto qitr = qgram_cache.find(qgram);
            if( qitr != qgram_cache.end()) { // found the qgram at the start of the current factor?
                const auto& qgram_list = qitr->second;
                bool found = false;
                size_t max_match_len = 0;
                local_offset = 0;
                uint32_t n = std::min(utils::max_pos_len,qgram_list.items);
                for(size_t i=0;i<n;i++) {
                    uint32_t p = qgram_list.pos[i];
                    auto tmp = start + p + sizeof(uint64_t); // we now the qgram matches!
                    auto pitr = factor_start + sizeof(uint64_t);
                    size_t match_len = sizeof(uint64_t);
                    while(tmp != factor_start && pitr != end && *tmp == *pitr) {
                        match_len++;
                        ++tmp;
                        ++pitr;
                    }
                    if(match_len > max_match_len) {
                        local_offset = p;
                        max_match_len = match_len;
                        found = true;
                    }
                }
                if(found && max_match_len > len) {
                    local = true;
                    len = max_match_len;
                    itr = factor_start + len;
                }
            }
        }
        // (4) update the local q-gram index
        {
            if(len >= sizeof(uint64_t)) {
                // utils::rlz_timer<std::chrono::microseconds> fbt("update q-gram");
                auto tmp = factor_start;
                uint64_t qgram = form_qgram(tmp);
                // LOG(INFO) << "INIT QGRAM = " << qgram;
                auto offset = std::distance(start,tmp);
                // LOG(INFO) << "INIT offset = " << offset;
                add_to_qgram_list(qgram,offset);
                offset += sizeof(qgram);
                tmp += sizeof(qgram);
                // LOG(INFO) << "tmp = " << std::distance(start,tmp);
                // LOG(INFO) << "itr = " << std::distance(start,itr);
                while(tmp != itr) {
                    update_qgram(qgram,tmp);
                    // LOG(INFO) << "update QGRAM = " << qgram;
                    add_to_qgram_list(qgram,offset);
                    offset++;
                    ++tmp;
                }
            }
        }
    }
    
    size_t max_qgram_len() {
        size_t max_len = 0;
        for(const auto& qg : qgram_cache) {
            size_t len = qg.second.items;
            max_len = std::max(len,max_len);
        }
        return max_len;
    }

    inline void find_next_factor()
    {
        sp = 0;
        ep = sa.size() - 1;
        while (itr != end) {
            sym = *itr;
            auto mapped_sym = sa.char2comp[sym];
            bool sym_exists_in_dict = mapped_sym != 0;
            uint64_t res_sp, res_ep;
            if (sym_exists_in_dict) {
                if (sp == 0 && ep == sa.size() - 1) {
                    // small optimization
                    res_sp = sa.C[mapped_sym];
                    res_ep = sa.C[mapped_sym + 1] - 1;
                }
                else {
                    sdsl::backward_search(sa, sp, ep, sym, res_sp, res_ep);
                }
            }
            if (!sym_exists_in_dict || res_ep < res_sp) {
                // FOUND FACTOR
                len = std::distance(factor_start, itr);
                if (len == 0) { // unknown symbol factor found
                    ++itr;
                }
                else {
                    // substring not found. but we found a factor!
                }
                find_longer_local_factor();
                factor_start = itr;
                return;
            }
            else { // found substring
                sp = res_sp;
                ep = res_ep;
                ++itr;
            }
        }
        /* are we in a substring? encode the rest */
        if (factor_start != itr) {
            len = std::distance(factor_start, itr);
            find_longer_local_factor();
            factor_start = itr;
            return;
        }
        done = true;
    }
    inline bool finished() const
    {
        return done;
    }
};

template <class t_csa, class t_itr>
struct factor_itr_csa_restricted {
    const t_csa& sa;
    t_itr factor_start;
    t_itr itr;
    t_itr end;
    uint64_t sp;
    uint64_t ep;
    uint64_t len;
    uint8_t sym;
    bool done;
    factor_itr_csa_restricted(const t_csa& _csa, t_itr begin, t_itr _end)
        : sa(_csa)
        , factor_start(begin)
        , itr(begin)
        , end(_end)
        , sp(0)
        , ep(_csa.size() - 1)
        , len(0)
        , sym(0)
        , done(false)
    {
        find_next_factor();
    }
    factor_itr_csa_restricted& operator++()
    {
        find_next_factor();
        return *this;
    }
    inline void find_next_factor()
    {
        sp = 0;
        ep = sa.size() - 1;
        while (itr != end) {
            sym = *itr;
            auto mapped_sym = sa.char2comp[sym];
            bool sym_exists_in_dict = mapped_sym != 0;
            uint64_t res_sp, res_ep;
            if (sym_exists_in_dict) {
                if (sp == 0 && ep == sa.size() - 1) {
                    // small optimization
                    res_sp = sa.C[mapped_sym];
                    res_ep = sa.C[mapped_sym + 1] - 1;
                }
                else {
                    sdsl::backward_search(sa, sp, ep, sym, res_sp, res_ep);
                }
            }
            if (!sym_exists_in_dict || res_ep <= res_sp) {
                // FOUND FACTOR
                len = std::distance(factor_start, itr);
                if (len == 0) { // unknown symbol factor found
                    ++itr;
                }
                else {
                    // substring not found. but we found a factor!
                }
                factor_start = itr;
                return;
            }
            else { // found substring
                sp = res_sp;
                ep = res_ep;
                ++itr;
            }
        }
        /* are we in a substring? encode the rest */
        if (factor_start != itr) {
            len = std::distance(factor_start, itr);
            factor_start = itr;
            return;
        }
        done = true;
    }
    inline bool finished() const
    {
        return done;
    }
};


template <class t_csa = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 4, 4096> >
struct dict_index_csa {
    typedef typename sdsl::int_vector<>::size_type size_type;
    t_csa sa;
    sdsl::rmq_succinct_sct<false> rmq;

    std::string type() const
    {
        return "dict_index_csa-" + sdsl::util::class_to_hash(*this);
    }

    dict_index_csa(collection& col, bool rebuild)
    {
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        auto file_name = col.path + "/index/" + type() + "-dhash=" + dict_hash + ".sdsl";
        if (!rebuild && utils::file_exists(file_name)) {
            LOG(INFO) << "\tDictionary index exists. Loading index from file.";
            std::ifstream ifs(file_name);
            load(ifs);
        }
        else {
            LOG(INFO) << "\tConstruct and store dictionary index";
            sdsl::cache_config cfg;
            cfg.delete_files = false;
            cfg.dir = col.path + "/tmp/";
            cfg.id = dict_hash + "-REV";
            auto reverse_dict_file = col.file_map[KEY_DICT] + ".rev";
            if (!utils::file_exists(reverse_dict_file) || rebuild) {
                LOG(INFO) << "\tStore reverse dictionary";
                sdsl::read_only_mapper<8> dict(col.file_map[KEY_DICT]);
                auto rdict = sdsl::write_out_buffer<8>::create(reverse_dict_file);
                rdict.resize(dict.size());
                // don't copy the 0 at the end
                std::reverse_copy(std::begin(dict), std::end(dict) - 1, std::begin(rdict));
                rdict[dict.size() - 1] = 0; // append zero for suffix sort
            }
            cfg.file_map[sdsl::conf::KEY_TEXT] = reverse_dict_file;
            LOG(INFO) << "\tConstruct index";
            sdsl::construct(sa, reverse_dict_file, cfg, 1);

            LOG(INFO) << "\tLoad uncompressed SA";
            sdsl::int_vector<> SA;
            load_from_cache(SA, sdsl::conf::KEY_SA, cfg);
            LOG(INFO) << "\tConstruct RMQ";
            rmq = sdsl::rmq_succinct_sct<false>(&SA);

            std::ofstream ofs(file_name);
            serialize(ofs);
        }
    }

    inline size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += sa.serialize(out, child, "csa");
        written_bytes += rmq.serialize(out, child, "rmq");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        sa.load(in);
        rmq.load(in);
    }

    template <class t_itr, bool t_search_local_block_context>
    factor_itr_csa<t_csa, t_itr, t_search_local_block_context> factorize(t_itr itr, t_itr end,std::unordered_map<uint64_t,utils::qgram_postings>& qgc) const
    {
        return factor_itr_csa<t_csa, t_itr, t_search_local_block_context>(sa, itr, end, qgc);
    }

    template <class t_itr>
    factor_itr_csa_restricted<t_csa, t_itr> factorize_restricted(t_itr itr, t_itr end) const
    {
        return factor_itr_csa_restricted<t_csa, t_itr>(sa, itr, end);
    }

    bool is_reverse() const
    {
        return true;
    }

    uint64_t find_minimum(uint64_t sp, uint64_t ep) const
    {
        if (sp != ep) {
            auto min = rmq(sp, ep);
            return sa[min];
        }
        return sa[sp];
    }
};
