#pragma once

#include <sdsl/int_vector.hpp>
#include <string>

template<class t_itr>
struct factor_itr_salcp {
    const sdsl::int_vector<0>& sa;
    const sdsl::int_vector<0>& lcp;
    const sdsl::int_vector<8>& text;
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
    				 const sdsl::int_vector<8>& _text,
    				 t_itr begin,t_itr _end)
        : sa(_sa),lcp(_lcp),text(_text),
          factor_start(begin), itr(begin), end(_end),
          sp(0), ep(_sa.size()-1), len(0), sym(0), done(false)
    {
        find_next_factor();
    }
    factor_itr_salcp& operator++() {
        find_next_factor();
        return *this;
    }
    inline void find_next_factor() {
    	/* TODO */
        done = true;
    }
    inline bool finished() const {
        return done;
    }
};

struct dict_index_salcp {
    typedef typename sdsl::int_vector<>::size_type size_type;
    sdsl::int_vector<> sa;
    sdsl::int_vector<> lcp;
    sdsl::int_vector<8> dict;

	std::string type() const {
		return "dict_index_salcp";
	}

    dict_index_salcp(collection& col,bool rebuild) {
    	auto dict_hash = col.param_map[PARAM_DICT_HASH];
    	auto file_name = col.path + "/index/"+type()+"-dhash="+dict_hash+".sdsl";
        if(!rebuild && utils::file_exists(file_name)) {
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
	        sdsl::load_from_cache(sa,sdsl::conf::KEY_SA,cfg);
	        sdsl::load_from_cache(lcp,sdsl::conf::KEY_LCP,cfg);
	        sdsl::load_from_cache(dict,sdsl::conf::KEY_TEXT,cfg);

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

    template<class t_itr>
    factor_itr_salcp<t_itr> factorize(t_itr itr,t_itr end) const {
        return factor_itr_salcp<t_itr>(sa,lcp,dict,itr,end);
    }

    bool is_reverse() const {
        return false;
    }
};
