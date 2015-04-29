#pragma once

#include <sdsl/int_vector.hpp>
#include <string>

template <class t_csa, class t_itr>
struct factor_itr_csa {
    const t_csa& sa;
    t_itr factor_start;
    t_itr itr;
    t_itr end;
    size_t sp;
    size_t ep;
    size_t len;
    uint8_t sym;
    bool done;
    factor_itr_csa(const t_csa& _csa, t_itr begin, t_itr _end)
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
    factor_itr_csa& operator++()
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
            size_t res_sp, res_ep;
            if (sym_exists_in_dict) {
                if (sp == 0 && ep == sa.size() - 1) {
                    // small optimization
                    res_sp = sa.C[mapped_sym];
                    res_ep = sa.C[mapped_sym + 1] - 1;
                } else {
                    sdsl::backward_search(sa, sp, ep, sym, res_sp, res_ep);
                }
            }
            if (!sym_exists_in_dict || res_ep < res_sp) {
                // FOUND FACTOR
                len = std::distance(factor_start, itr);
                if (len == 0) { // unknown symbol factor found
                    ++itr;
                } else {
                    // substring not found. but we found a factor!
                }
                factor_start = itr;
                return;
            } else { // found substring
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
        } else {
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
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        sa.load(in);
    }

    template <class t_itr>
    factor_itr_csa<t_csa, t_itr> factorize(t_itr itr, t_itr end) const
    {
        return factor_itr_csa<t_csa, t_itr>(sa, itr, end);
    }

    bool is_reverse() const
    {
        return true;
    }
};