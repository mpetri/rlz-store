#pragma once

#include "utils.hpp"
#include "collection.hpp"

template <uint32_t t_block_size_bytes>
class dict_uniform_sample_budget {
public:
    static std::string type()
    {
        return "dict_uniform_sample_budget-"
            + std::to_string(t_block_size_bytes);
    }

    static std::string file_name(collection& col, uint64_t size_in_bytes)
    {
        auto size_in_mb = size_in_bytes / (1024 * 1024);
        return col.path + "/index/" + type() + "-" + std::to_string(size_in_mb) + ".sdsl";
    }

public:
    static void create(collection& col, bool rebuild, size_t size_in_bytes)
    {
        const uint32_t block_size = t_block_size_bytes;
        const uint8_t sep_symbol = 0x01;
        uint64_t budget_bytes = size_in_bytes;
        uint64_t budget_mb = budget_bytes / (1024 * 1024);
        // check if we store it already and load it
        auto fname = file_name(col, size_in_bytes);
        col.file_map[KEY_DICT] = fname;
        if (!utils::file_exists(fname) || rebuild) { // construct
            auto start_total = hrclock::now();
            LOG(INFO) << "\tCreate dictionary with budget "<< budget_bytes 
                      <<" (" << budget_mb << " MiB)";
            // memory map the text and iterate over it
            std::vector<uint8_t> dict;
            {
                sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
                auto num_samples = budget_bytes / block_size;
                LOG(INFO) << "\tDictionary samples = " << num_samples;
                auto n = text.size();
                size_t sample_step = n / num_samples;
                LOG(INFO) << "\tSample steps = " << sample_step;
                for (size_t i = 0; i < n; i += sample_step) {
                    for (size_t j = 0; j < (block_size-1); j++) {
                        if (i + j >= (n-1))
                            break;
                        if(dict.size()+3 >= budget_bytes) break;
                        dict.push_back(text[i + j]);
                    }
                    dict.push_back(sep_symbol); // add sep symbol between selected segments
                    if(dict.size()+2 >= budget_bytes) break;
                }
                dict.push_back(0); // zero terminate for SA construction
            }
            LOG(INFO) << "\tdict size in bytes = " << dict.size();
            /* store to disk */
            LOG(INFO) << "\t"
                      << "Writing dictionary.";
            auto wdict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
            std::copy(dict.begin(), dict.end(), std::back_inserter(wdict));
            auto end_total = hrclock::now();
            LOG(INFO) << "\t" << type() + " Total time = " << duration_cast<milliseconds>(end_total - start_total).count() / 1000.0f << " sec";
        } else {
            LOG(INFO) << "\t"
                      << "Dictionary exists at '" << fname << "'";
        }
        // compute a hash of the dict so we don't reconstruct things
        // later when we don't have to.
        col.compute_dict_hash();
    }
};
