#pragma once

#include <sdsl/int_vector.hpp>
#include <string>
#include <vector>

//this class tries to select a factor with the maximum possible length "factor-length" with the constraint that "factor-length" is a multiple of LM
// [sp-ep] then gives the range of factors valid with that length.
//LM stands for length multiplier . With the default value of 1, all factor lengths are valid and this is the same strategy as the greedy algorithm.
//

template <class t_itr>
struct factor_itr_sa_length_selector {
    const sdsl::int_vector<0>& sa;
    const sdsl::int_vector<0>& lcp;
    const sdsl::int_vector<8>& dictionary;
    t_itr factor_start;
    t_itr itr;
    t_itr end;
    size_t sp;
    size_t ep;
    size_t len;
    uint8_t sym;
    bool done;
    int LM;
    factor_itr_sa_length_selector(const sdsl::int_vector<0>& _sa,
                                  const sdsl::int_vector<0>& _lcp,
                                  const sdsl::int_vector<8>& _dictionary,
                                  t_itr begin, t_itr _end, int _lm)
        : sa(_sa)
        , lcp(_lcp)
        , dictionary(_dictionary)
        , factor_start(begin)
        , itr(begin)
        , end(_end)
        , sp(0)
        , ep(_sa.size() - 1)
        , len(0)
        , sym(0)
        , done(false)
        , LM(_lm)
    {
        find_next_factor();
    }
    factor_itr_sa_length_selector& operator++()
    {
        find_next_factor();
        return *this;
    }

    struct searchInAColumn {
        const sdsl::int_vector<8>& dictionary;
        const t_itr& query_end;
        const size_t matchPosition; //search only at this matchPosition - rlz refine() algorithm

        searchInAColumn(const sdsl::int_vector<8>& _dictionary, const t_itr& _qe, const size_t& _mp)
            : dictionary(_dictionary)
            , query_end(_qe)
            , matchPosition(_mp)
        {
        }

        bool operator()(const int& suffix_value, t_itr query_ptr)
        {

            t_itr suffix_ptr = dictionary.begin() + suffix_value + matchPosition;
            query_ptr += matchPosition;

            if (suffix_ptr > dictionary.end()) {
                //suffix string is empty at match pos => it is lesser
                return true;
            }
            return (*suffix_ptr < *query_ptr);
        }

        bool operator()(t_itr query_ptr, const int& suffix_value)
        {
            t_itr suffix_ptr = dictionary.begin() + suffix_value + matchPosition;
            query_ptr += matchPosition;

            if (suffix_ptr > dictionary.end()) {
                //suffix string is empty at match pos => it is lesser
                return true;
            }
            return (*query_ptr < *suffix_ptr);
        }
    };

    void find_next_factor()
    {
        if (itr >= end) {
            done = true; //nothing to process .. all finished
            return;
        }

        struct factorInfo {
            size_t sp;
            size_t ep;
            size_t matchlength;
        };
        std::vector<factorInfo> factorRanges;
        auto rangeStart = sa.begin();
        auto rangeEnd = sa.end();
        size_t match_length = 0;
        while (true) //rlz-refine loop here
        {
            auto sfx_left = std::lower_bound(rangeStart, rangeEnd, itr, searchInAColumn(dictionary, end, match_length));
            auto sfx_right = std::upper_bound(rangeStart, rangeEnd, itr, searchInAColumn(dictionary, end, match_length));

            //check if we got a valid subrange -- ie a column full of matching characters
            if ((sfx_left == rangeEnd) || (dictionary[*sfx_left + match_length] != itr[match_length])) {
                break;
            }

            //otherwise valid range
            {
                size_t lb = std::distance(sa.begin(), sfx_left);
                size_t rb = std::distance(sa.begin(), sfx_right) - 1; //-1 because sfx_right points to the first entry > than query

                factorRanges.push_back(factorInfo{ lb, rb, ++match_length });
                rangeStart = sfx_left;
                rangeEnd = sfx_right;

                // go back to loop and look at the next matchlength/matchPosition column
                //unless we have ran out of the block
                size_t dist = std::distance(itr, end);
                if (dist <= match_length) {
                    break;
                }
            }
        }

        for (auto factor_iterator = factorRanges.rbegin(); factor_iterator != factorRanges.rend(); ++factor_iterator) {
            if ((factor_iterator->matchlength % LM) == 0) //found a factor whose match length is a multiple of LM
            {
                len = factor_iterator->matchlength;
                sp = factor_iterator->sp;
                ep = factor_iterator->ep;
                itr += len;
                return;
            }
        }

        //default option -- select no factor ( matchlength ==0)
        len = 0;
        sym = *itr;
        ++itr;
        return;

        /*factor is the one at the top of the list
        if (factorRanges.size() <= 0) {
            len = 0;
            sym = *itr;
            ++itr;
            LOG(TRACE) << "found factor(0)  " << sym << "," << len;
            return;
        } else {
            len = factorRanges.back().matchlength;
            sp = factorRanges.back().sp;
            ep = factorRanges.back().ep;
            itr += len;

            LOG(TRACE) << "found factor  " << sa[sp] << "," << sa[ep] << "," << len <<" from Range "<< sp <<","<<ep;
            return;
        }*/
    }
    inline bool finished() const
    {
        return done;
    }
};

template <int t_LM = 1>
struct dict_index_sa_length_selector {
    typedef typename sdsl::int_vector<>::size_type size_type;
    sdsl::int_vector<> sa;
    sdsl::int_vector<> lcp;
    sdsl::int_vector<8> dict;

    std::string type() const
    {
        return "dict_index_sa_length_selector";
    }

    dict_index_sa_length_selector(collection& col, bool rebuild)
    {
        auto dict_hash = col.param_map[PARAM_DICT_HASH];
        auto file_name = col.path + "/index/" + type() + "-dhash=" + dict_hash + ".sdsl";
        if (!rebuild && utils::file_exists(file_name)) {
            LOG(INFO) << "\tDictionary index exists. Loading index from file.";
            std::ifstream ifs(file_name);
            load(ifs);
        } else {
            LOG(INFO) << "\tConstruct and store dictionary index";
            sdsl::cache_config cfg;
            cfg.delete_files = false;
            cfg.dir = col.path + "/tmp/";
            cfg.id = dict_hash;
            cfg.file_map[sdsl::conf::KEY_TEXT] = col.file_map[KEY_DICT];
            LOG(INFO) << "\tConstruct SA";
            if (!sdsl::cache_file_exists(sdsl::conf::KEY_SA, cfg)) {
                sdsl::construct_sa<8>(cfg);
            }
            LOG(INFO) << "\tConstruct BWT";
            if (!sdsl::cache_file_exists(sdsl::conf::KEY_BWT, cfg)) {
                sdsl::construct_bwt<8>(cfg);
            }
            LOG(INFO) << "\tConstruct LCP";
            if (!sdsl::cache_file_exists(sdsl::conf::KEY_LCP, cfg)) {
                sdsl::construct_lcp_semi_extern_PHI(cfg);
            }

            LOG(INFO) << "\tLoad SA/LCP/DICT";
            sdsl::load_from_cache(sa, sdsl::conf::KEY_SA, cfg);
            sdsl::load_from_cache(lcp, sdsl::conf::KEY_LCP, cfg);
            sdsl::load_from_cache(dict, sdsl::conf::KEY_TEXT, cfg);

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
        written_bytes += lcp.serialize(out, child, "lcp");
        written_bytes += dict.serialize(out, child, "dict");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        sa.load(in);
        lcp.load(in);
        dict.load(in);
    }

    template <class t_itr>
    factor_itr_sa_length_selector<t_itr> factorize(t_itr itr, t_itr end) const
    {
        return factor_itr_sa_length_selector<t_itr>(sa, lcp, dict, itr, end, t_LM);
    }

    bool is_reverse() const
    {
        return false;
    }

    uint64_t find_minimum(size_t sp, size_t ep) const
    {
        if (sp != ep) {
            auto min_itr = std::min_element(sa.begin() + sp, sa.begin() + ep + 1);
            return *min_itr;
        }
        return sa[sp];
    }
};
