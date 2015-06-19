#pragma once

#include "utils.hpp"
#include "collection.hpp"

template <uint32_t t_block_size_bytes = 32>
class dict_random_uniform_hash {
public:
    static std::string type()
    {
        return "dict_random_uniform_hash-"
               + std::to_string(t_block_size_bytes);
    }

    static std::string file_name(collection& col,uint64_t size_in_bytes)
    {
        auto size_in_mb = size_in_bytes / (1024 * 1024);
        return col.path + "/index/" + type() + "-" + std::to_string(size_in_mb) + ".sdsl";
    }

public:
	
    static void create(collection& col, bool rebuild,size_t size_in_bytes)
    {
        const uint32_t block_size = t_block_size_bytes;
        uint64_t budget_bytes = size_in_bytes;
        uint64_t budget_mb = budget_bytes / (1024*1024);

        // create a random dictionary afresh, for every run 
        auto fname = file_name(col,size_in_bytes);
        col.file_map[KEY_DICT] = fname;
        std::vector<uint8_t> dict;
		{
            LOG(INFO) << "\tCreate dictionary with budget " << budget_mb << " MiB";

            sdsl::int_vector<8> text;
            sdsl::load_from_file(text,col.file_map[KEY_TEXT]);
            auto num_samples = budget_bytes / block_size;
            auto n = text.size();
            LOG(INFO) << "\tDictionary samples = " << num_samples;
	
			std::vector<uint32_t> sample_ids(n / block_size) ; 
			for (uint32_t i=0; i < (n/block_size); i++) sample_ids[i] = i;

			std::mt19937 g(1234);
			std::shuffle(sample_ids.begin(),sample_ids.end(), g);

			std::unordered_map<std::string,uint64_t> included_blocks;

			for(const auto id : sample_ids) {
				// (1) get block content
				auto beg = text.begin()+id*block_size;
				auto end = beg + block_size;
				std::string block_content(beg,end);
				auto itr = included_blocks.find(block_content);
				if(itr == included_blocks.end()) {
					// block not found. ADD IT if we have not found enough!
					if( included_blocks.size() < num_samples ) {
						included_blocks[block_content] = id;
					}
				} else {
					if( itr->second > id)
						itr->second = id;
				}
			}
			std::vector<uint64_t> block_ids;
			for(const auto& sb : included_blocks) {
				block_ids.push_back(sb.second);
			}
			std::sort(block_ids.begin(),block_ids.end());

			auto wdict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
			for(const auto& id : block_ids) {
				auto beg = text.begin() + id * block_size;
				auto end = beg + block_size;
				 std::copy(beg,end,std::back_inserter(wdict));
			}
			dict.push_back(0);           
        }
	
	    // compute a hash of the dict so we don't reconstruct things
        // later when we don't have to.
        col.compute_dict_hash();
    }
};
