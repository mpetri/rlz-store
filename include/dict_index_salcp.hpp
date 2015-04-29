#pragma once

#include <sdsl/int_vector.hpp>
#include <string>

template <class t_itr>
struct factor_itr_salcp {
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
    factor_itr_salcp(const sdsl::int_vector<0>& _sa,
                     const sdsl::int_vector<0>& _lcp,
                     const sdsl::int_vector<8>& _dictionary,
                     t_itr begin, t_itr _end)
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
    {
        find_next_factor();
    }
    factor_itr_salcp& operator++()
    {
        find_next_factor();
        return *this;
    }

    struct binSearchWithSA {
        const sdsl::int_vector<8>& dictionary;
        const t_itr& query_end;

        binSearchWithSA(const sdsl::int_vector<8>& _dictionary, const t_itr& _qe)
            : dictionary(_dictionary)
            , query_end(_qe){};

        bool operator()(const int& suffix_value, t_itr query_ptr)
        {
            return std::lexicographical_compare(dictionary.begin() + suffix_value, dictionary.end(), query_ptr, query_end);
        }
    };

    inline void find_next_factor()
    {
        if (itr >= end) {
            done = true; //nothing to process .. all finished
            return;
        }

        size_t match_len_high = 0;
        size_t match_len_low = 0;
        auto query_length = end - itr;

        auto sfx_high = std::lower_bound(sa.begin(), sa.end(), itr, binSearchWithSA(dictionary, end));
        auto sfx_low = sfx_high;
        //query's rank is sfx_high in sa

        if (sfx_high < sa.end()) {
            //find match length with this suffix
            auto suffix_itr = dictionary.begin() + *sfx_high;

            auto suffix_length = dictionary.end() - suffix_itr;
            match_len_high = std::mismatch(suffix_itr, suffix_itr + std::min(suffix_length, query_length), itr).first - suffix_itr;
        }

        if (sfx_high > sa.begin() && sfx_high < sa.end()) {
            //find match length with a suffix at previous suffix (low)
            sfx_low = sfx_high - 1;
            auto suffix_itr = dictionary.begin() + *sfx_low;

            auto suffix_length = dictionary.end() - suffix_itr;
            match_len_low = std::mismatch(suffix_itr, suffix_itr + std::min(suffix_length, query_length), itr).first - suffix_itr;
        }

        if (match_len_high > 0 || match_len_low > 0) {
            if (match_len_high > match_len_low) {
                len = match_len_high;
                sp = ep = (sfx_high - sa.begin());
                itr += len;
            } else {
                len = match_len_low;
                sp = ep = (sfx_low - sa.begin());
                itr += len;
            }
            //LOG(TRACE) <<"found factor  "<< sp << "," <<len;
        } else {
            len = 0;
            sym = *itr;
            itr++;
            //LOG(TRACE) <<"found factor(0)  "<< sym << ","<<len;
        }
        /*

       	if(itr != end) {
	    len = 0;
	    sym = *itr;
	    ++itr;
	    return;
	}
    	// TODO 
        done = true;
	*/
    }
    inline bool finished() const
    {
        return done;
    }
};

struct dict_index_salcp {
    typedef typename sdsl::int_vector<>::size_type size_type;
    sdsl::int_vector<> sa;
    sdsl::int_vector<> lcp;
    sdsl::int_vector<8> dict;

    std::string type() const
    {
        return "dict_index_salcp";
    }

    dict_index_salcp(collection& col, bool rebuild)
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
    factor_itr_salcp<t_itr> factorize(t_itr itr, t_itr end) const
    {
        return factor_itr_salcp<t_itr>(sa, lcp, dict, itr, end);
    }

    bool is_reverse() const
    {
        return false;
    }
};
