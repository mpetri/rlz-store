#pragma once

#include <sdsl/int_vector.hpp>
#include <string>

template<
class t_csa = sdsl::csa_wt< sdsl::wt_huff<sdsl::bit_vector_il<64>> , 4 , 4096 >;
>
struct dict_index_csa {
    typedef typename sdsl::int_vector<>::size_type size_type;
    t_csa m_csa;

	std::string type() const {
		return "dict_index_csa-"+sdsl::util::class_to_hash(*this);
	}

    dict_index_csa(collection& col) {
    	auto dict_hash = col.param_map[PARAM_DICT_HASH];
    	auto file_name = col.path + "/index/"+type()+"-dhash="+dict_hash+".sdsl";
        if(!col.rebuild && utils::file_exists(file_name)) {
        	LOG(INFO) << "\tDictionary index exists. Loading index from file.";
        	std::ifstream ifs(file_name);
        	load(ifs);
        } else {
	        LOG(INFO) << "\tConstruct and store dictionary index to " << file_name;
	        sdsl::cache_config cfg;
	        cfg.delete_files = false;
	        cfg.dir = col.path + "/tmp/";
	        cfg.id = dict_hash;
            auto reverse_dict_file = col.file_map[KEY_DICT]+".rev";
            if(! utils::file_exists(reverse_dict_file) || col.rebuild) {
                LOG(INFO) << "\tStore reverse dictionary";
                sdsl::read_only_mapper<8> dict(col.file_map[KEY_DICT]);
                auto rdict = sdsl::write_out_buffer<8>::create(reverse_dict_file);
                rdict.resize(dict.size());
                // don't copy the 0 at the end
                std::reverse_copy(std::begin(dict), std::end(dict)-1, std::begin(rdict));
                rdict[dict.size()-1] = 0; // append zero for suffix sort
            }
            cfg.file_map[sdsl::conf::KEY_TEXT] = reverse_dict_file;
            LOG(INFO) << "\tConstruct index";
            sdsl::construct(m_csa,reverse_dict_file,cfg,1);
            std::ofstream ofs(file_name);
            serialize(ofs);
	    }
    }

    inline size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += m_csa.serialize(out, child, "csa");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        m_csa.load(in);
    }

};
