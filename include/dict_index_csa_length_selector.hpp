#pragma once

#include <sdsl/int_vector.hpp>
#include <string>
#include <sdsl/rmq_support.hpp>

template <class t_csa, class t_itr>
struct factor_itr_csa_len_select {
    const t_csa& sa;
    uint32_t len_multiplier;
    t_itr factor_start;
    t_itr start;
    t_itr itr;
    t_itr end;
    size_t sp;
    size_t ep;
    size_t len;
    uint8_t sym;
    bool done;
    factor_itr_csa_len_select(const t_csa& _csa, t_itr begin, t_itr _end,uint32_t lm)
        : sa(_csa)
        , len_multiplier(lm)
        , factor_start(begin)
        , start(begin)
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
    factor_itr_csa_len_select& operator++()
    {
        find_next_factor();
        return *this;
    }
    inline void find_next_factor()
    {
        sp = 0;
        ep = sa.size() - 1;
        size_t lm_sp = 0; size_t lm_ep = 0; size_t lm_len = 0; t_itr lm_pos = itr;
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
                if(len % len_multiplier == 0) {
	                if (len == 0) { // unknown symbol factor found
	                    ++itr;
	                } else {
	                    // substring not found. but we found a factor!
	                }
                	factor_start = itr;
                } else {
                	if(lm_len != 0) {
                		// fall back to the last good factor length
                		sp = lm_sp;
                		ep = lm_ep;
                		len = lm_len;
                		itr = lm_pos + 1;
                	} else {
                		// nothing to fall back on. encode a singleton
                		len = 0;
                		sym = *lm_pos;
                		itr = lm_pos + 1;
                	}
                	factor_start = itr;
                }
                return;
            } else { // found substring
            	auto l = std::distance(factor_start, itr) + 1;
            	if(l % len_multiplier == 0) {
            		lm_sp = res_sp;
            		lm_ep = res_ep;
            		lm_len = l;
            		lm_pos = itr;
            	}
                sp = res_sp;
                ep = res_ep;
                ++itr;
            }
        }
        /* end of block handling: are we in a substring? encode the rest */
        if (factor_start != itr) {
            len = std::distance(factor_start, itr);
            if(len % len_multiplier == 0) {
                // encode the last factor as normal
            } else {
            	if(lm_len != 0) {
            		// fall back to the last good factor length
            		sp = lm_sp;
            		ep = lm_ep;
            		len = lm_len;
            		itr = lm_pos + 1;
            	} else {
            		// nothing to fall back on. encode a singleton
            		len = 0;
            		sym = *lm_pos;
            		itr = lm_pos + 1;
            	}
            }
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

template <
uint32_t t_len,
class t_csa = sdsl::csa_wt<sdsl::wt_huff<sdsl::bit_vector_il<64> >, 4, 4096> >
struct dict_index_csa_length_selector {
    typedef typename sdsl::int_vector<>::size_type size_type;
    t_csa sa;
    sdsl::rmq_succinct_sct<false> rmq;

    std::string type() const
    {
        return "dict_index_csa_length_selector-" + sdsl::util::class_to_hash(*this);
    }

    dict_index_csa_length_selector(collection& col, bool rebuild)
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

    template <class t_itr>
    factor_itr_csa_len_select<t_csa, t_itr> factorize(t_itr itr, t_itr end) const
    {
        return factor_itr_csa_len_select<t_csa, t_itr>(sa, itr, end,t_len);
    }

    bool is_reverse() const
    {
        return true;
    }

    uint64_t find_minimum(size_t sp,size_t ep) const {
        if(sp != ep) {
            auto min = rmq(sp,ep);
            return sa[min];
        }
        return sa[sp];
    }
};
