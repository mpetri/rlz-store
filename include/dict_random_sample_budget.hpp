#pragma once

#include "utils.hpp"
#include "collection.hpp"

template <uint32_t t_budget_mb = 1024,
          uint32_t t_block_size_bytes = 1024>
class dict_random_sample_budget {
public:
    static std::string type()
    {
        return "dict_random_sample_budget-"
               + std::to_string(t_budget_mb) + "-"
               + std::to_string(t_block_size_bytes);
    }

public:
    static void create(collection& col, bool rebuild)
    {
        const uint32_t block_size = t_block_size_bytes;
        const uint32_t budget_bytes = t_budget_mb * (1024 * 1024);
        // check if we store it already and load it
        auto file_name = col.path + "/index/" + type() + ".sdsl";
        col.file_map[KEY_DICT] = file_name;
        if (!utils::file_exists(file_name) || rebuild) { // construct
            LOG(INFO) << "\t"
                      << "Create dictionary with budget " << t_budget_mb << " MiB";
            // memory map the text and iterate over it
            std::vector<uint8_t> dict;
            {
                sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
                auto num_samples = budget_bytes / block_size;
                LOG(INFO) << "\t"
                          << "Dictionary samples = " << num_samples;
                auto n = text.size();
                size_t sample_step = n / num_samples;
                LOG(INFO) << "\t"
                          << "Sample steps = " << sample_step;
                for (size_t i = 0; i < n; i += sample_step) {
                    for (size_t j = 0; j < block_size; j++) {
                        if (i + j >= n)
                            break;
                        dict.push_back(text[i + j]);
                    }
                }
                dict.push_back(0); // zero terminate for SA construction
            }
            /* store to disk */
            LOG(INFO) << "\t" << "Writing dictionary.";
            auto wdict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
            std::copy(dict.begin(),dict.end(),std::back_inserter(wdict));
        } else {
            LOG(INFO) << "\t"
                      << "Dictionary exists at '" << file_name << "'";
        }
        // compute a hash of the dict so we don't reconstruct things
        // later when we don't have to.
        col.compute_dict_hash();
    }
};
