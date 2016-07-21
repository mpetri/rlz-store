#pragma once

#include <sdsl/int_vector.hpp>
#include <string>
#include <sdsl/rmq_support.hpp>

template <class t_itr, bool t_local_search>
struct factor_itr_sa {
    const sdsl::int_vector<>& sa;
    const sdsl::int_vector<8>& text;
    const sdsl::int_vector<>& cache;
    decltype(sa.begin()) sa_start;
    decltype(text.begin()) text_start;
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

    // std::unordered_map<uint32_t,std::vector<uint32_t>> qgrams;
    factor_itr_sa(const sdsl::int_vector<>& _sa, const sdsl::int_vector<8>& _text, const sdsl::int_vector<>& _cache, t_itr begin, t_itr _end)
        : sa(_sa)
        , text(_text)
        , cache(_cache)
        , sa_start(sa.begin())
        , text_start(text.begin())
        , factor_start(begin)
        , itr(begin)
        , start(begin)
        , end(_end)
        , sp(0)
        , ep(_sa.size() - 1)
        , len(0)
        , local_offset(0)
        , sym(0)
        , done(false)
        , local(false)
    {
        find_next_factor();
        //qgrams.reserve(64000);
    }
    factor_itr_sa& operator++()
    {
        find_next_factor();
        return *this;
    }

    inline void find_longer_local_factor()
    {
        local = false;
        // (1) local search enabled?
        if (!t_local_search)
            return;
        // (2) is the global factor already quite long? are we in the first part of the block?
        // if( std::distance(start,itr) > 1024 && len >= 20) return;

        // // (3) search for a better factor locally
        // {
        //     utils::rlz_timer<std::chrono::nanoseconds> fbt("search q-gram");
        //     uint32_t qgram = 0; uint8_t* qg8 = (uint8_t*) &qgram;
        //     qg8[0] = *factor_start; qg8[1] = *(factor_start+1); qg8[2] = *(factor_start+2);  qg8[3] = *(factor_start+3);
        //     auto qitr = qgrams.find(qgram);
        //     if( qitr != qgrams.end()) { // found the qgram at the start of the current factor?
        //         const auto& qpos = qitr->second;
        //         bool found = false;
        //         size_t max_match_len = 0;
        //         local_offset = 0;
        //         for(const auto& p : qpos) { // check all pos of qgram for better matches
        //             auto tmp = start + p;
        //             auto pitr = factor_start;
        //             size_t match_len = 0;
        //             while(tmp != factor_start && pitr != end && *tmp == *pitr) {
        //                 match_len++;
        //                 ++tmp;
        //                 ++pitr;
        //             }
        //             if(match_len > max_match_len) {
        //                 local_offset = p;
        //                 max_match_len = match_len;
        //                 found = true;
        //             }
        //         }
        //         if(found && max_match_len > len) {
        //             local = true;
        //             len = max_match_len;
        //             itr = factor_start + len;
        //         }
        //     }
        // }
        // // (4) update the local q-gram index
        // {
        //     utils::rlz_timer<std::chrono::nanoseconds> fbt("update q-gram");
        //     uint32_t qgram = 0; uint8_t* qg8 = (uint8_t*) &qgram;
        //     auto tmp = factor_start;
        //     size_t syms_seen = 0;
        //     while(tmp != itr) {
        //         qg8[syms_seen] = *tmp;
        //         syms_seen++;
        //         if(syms_seen == 4) {
        //             auto offset = std::distance(start,tmp) - 3;
        //             qgrams[qgram].push_back(offset);
        //             qg8[0] = qg8[1];
        //             qg8[1] = qg8[2];
        //             qg8[2] = qg8[3];
        //             syms_seen = 3;
        //         }
        //         ++tmp;
        //     }
        // }
    }

    bool refine_bounds(uint64_t& lb, uint64_t& rb, uint8_t pat_sym, size_t offset)
    {
        auto left = sa_start + lb;
        auto right = sa_start + rb;
        auto count = std::distance(left, right);
        while (count > 0) {
            auto step = count / 2;
            auto mid = left + step;
            uint8_t dict_sym = *(text_start + *mid + offset);
            if (dict_sym < pat_sym) {
                count -= step + 1;
                left = ++mid;
            }
            else {
                count = step;
            }
        }
        auto sp = left;
        left = sa_start + lb;
        right = sa_start + rb;
        count = std::distance(left, right);
        while (count > 0) {
            auto step = count / 2;
            auto mid = left + step;
            uint8_t dict_sym = *(text_start + *mid + offset);
            if (dict_sym <= pat_sym) {
                count -= step + 1;
                left = ++mid;
            }
            else {
                count = step;
            }
        }
        auto ep = left;
        uint8_t dict_sym = *(text_start + *left + offset);
        if (dict_sym != pat_sym)
            ep--;

        if (sp <= ep) {
            lb = std::distance(sa_start, sp);
            rb = std::distance(sa_start, ep);
            return true;
        }
        return false;
    }

    inline void find_next_factor()
    {
        if (itr == end) {
            done = true;
            return;
        }
        sp = 0;
        ep = sa.size() - 1;
        size_t offset = 0;

        // ask the cache!
        if (std::distance(itr, end) >= 3) {
            uint32_t two_gram = uint32_t(*itr) << 16 | uint32_t(*(itr + 1)) << 8 | uint32_t(*(itr + 2));
            sp = cache[two_gram * 2];
            ep = cache[two_gram * 2 + 1];
            if (ep < sp) {
                // len = 0;
                // ++itr;
                // factor_start = itr;
                // return;
                sp = 0;
                ep = sa.size() - 1;
            }
            else {
                offset += 3;
                itr += 3;
            }
        }

        /* refine bounds as long as possible */
        while (itr != end && refine_bounds(sp, ep, *itr, offset)) {
            ++itr;
            ++offset;
            if (sp == ep)
                break;
        }
        if (sp == ep) {
            auto text_itr = text_start + sa[sp] + offset;
            while (itr != end && *text_itr == *itr) {
                ++itr;
                ++text_itr;
                ++offset;
            }
        }

        len = offset;
        if (len == 0) { // unknown symbol factor found
            ++itr;
        }
        factor_start = itr;
    }
    inline bool finished() const
    {
        return done;
    }
};

struct dict_index_sa {
    typedef typename sdsl::int_vector<>::size_type size_type;
    sdsl::int_vector<> sa;
    sdsl::int_vector<8> text;
    sdsl::int_vector<> cache;

    std::string type() const
    {
        return "dict_index_sa-" + sdsl::util::class_to_hash(*this);
    }

    dict_index_sa(collection& col, bool rebuild)
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
            LOG(INFO) << "\tConstruct suffix array";
            sdsl::load_from_file(text, col.file_map[KEY_DICT]);
            sa.width(sdsl::bits::hi(text.size()) + 1);
            sdsl::algorithm::calculate_sa((const uint8_t*)text.data(), text.size(), sa);

            //
            LOG(INFO) << "\tCompute a 3-gram cache";
            {
                size_t num_kgrams = 256 * 256 * 256;
                sdsl::int_vector<> counts(num_kgrams);
                uint32_t cur_k_gram = uint32_t(text[0]) << 16 | uint32_t(text[1]) << 8 | uint32_t(text[2]);
                counts[cur_k_gram]++;
                for (size_t i = 3; i < text.size(); i++) {
                    cur_k_gram = ((cur_k_gram << 8) & 0xFFFF00) | uint32_t(text[i]);
                    counts[cur_k_gram]++;
                }

                /* fix up some counts at the end */
                counts[0] = 1;
                cur_k_gram = uint32_t(text[text.size() - 2]) << 16 | uint32_t(text[text.size() - 1]) << 8 | uint32_t(text[text.size() - 1]);
                counts[cur_k_gram]++;

                sdsl::int_vector<> ccounts(num_kgrams);
                ccounts[0] = 0;

                for (size_t i = 1; i < num_kgrams; i++)
                    ccounts[i] = ccounts[i - 1] + counts[i - 1];

                cache.resize(num_kgrams * 2);
                for (size_t i = 0; i < num_kgrams; i++) {
                    cache[i * 2] = ccounts[i];
                    cache[i * 2 + 1] = ccounts[i] + counts[i] - 1;
                    if (counts[i] == 0) {
                        cache[i * 2] += 2;
                        cache[i * 2 + 1] += 1;
                    }
                }
                sdsl::util::bit_compress(cache);
            }
            LOG(INFO) << "\tWrite index to disk";
            std::ofstream ofs(file_name);
            serialize(ofs);
        }
    }

    inline size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += sa.serialize(out, child, "sa");
        written_bytes += text.serialize(out, child, "text");
        written_bytes += cache.serialize(out, child, "cache");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        sa.load(in);
        text.load(in);
        cache.load(in);
    }

    template <class t_itr, bool t_search_local_block_context>
    factor_itr_sa<t_itr, t_search_local_block_context> factorize(t_itr itr, t_itr end) const
    {
        return factor_itr_sa<t_itr, t_search_local_block_context>(sa, text, cache, itr, end);
    }

    bool is_reverse() const
    {
        return false;
    }
};
