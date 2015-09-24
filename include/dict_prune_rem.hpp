#pragma once

#include <ratio>

#include "utils.hpp"
#include "collection.hpp"
#include "factor_storage.hpp"

template <uint32_t t_block_size_bytes>
struct dict_prune_rem {
    static std::string type()
    {
        return "dict_pruned_rem-" + std::to_string(t_block_size_bytes);
    }

    static std::string file_name(collection& col, uint64_t size_in_bytes,std::string dhash)
    {
        auto size_in_mb = size_in_bytes / (1024 * 1024);
        return col.path + "/index/" + type() + "-" + std::to_string(size_in_mb) + "-dhash=" + dhash +  ".sdsl";
    }


    template <class t_dict_idx,class t_factorization_strategy>
    static void prune(collection& col, bool rebuild,uint64_t target_dict_size_bytes,uint64_t num_threads)
    {
        auto start_total = hrclock::now();
    	auto start_dict_size_bytes = 0ULL;
    	{
    		const sdsl::int_vector_mapper<8, std::ios_base::in> dict(col.file_map[KEY_DICT]);
    		start_dict_size_bytes = dict.size();
    	}
    	if(start_dict_size_bytes == target_dict_size_bytes) {
        	LOG(INFO) << "\t"
                  	<< "No dictionary pruning necessary.";
            return;
    	}
    	auto dict_hash = col.param_map[PARAM_DICT_HASH];
    	auto new_dict_file = file_name(col,target_dict_size_bytes,dict_hash);
    	if (!rebuild && utils::file_exists(new_dict_file)) {
            LOG(INFO) << "\t"
                      << "Pruned dictionary exists at '" << new_dict_file << "'";
    		col.file_map[KEY_DICT] = new_dict_file;
    		col.compute_dict_hash();
    		return;
    	}

    	/* (1) create or load statistics */
        LOG(INFO) << "\t" << "Create or load statistics.";
    	auto dict_file = col.file_map[KEY_DICT];
    	auto dict_stats_file = dict_file + "-" + KEY_DICT_STATISTICS + "-" + t_factorization_strategy::type() + ".sdsl";
    	factorization_statistics fstats;
    	if (rebuild || !utils::file_exists(dict_stats_file)) {
	        fstats = t_factorization_strategy::template parallel_factorize<factor_tracker>(col,rebuild,num_threads);
	        sdsl::store_to_file(fstats,dict_stats_file);
	    } else {
	    	sdsl::load_from_file(fstats,dict_stats_file);
	    }

    	/* (2) create block statistics */
    	LOG(INFO) << "\t" << "Create block statistics.";
    	auto start_num_blocks = start_dict_size_bytes / t_block_size_bytes;
    	auto left = start_dict_size_bytes % t_block_size_bytes;
    	if(left) start_num_blocks++;
    	struct block_stat {
    		uint64_t offset;
    		uint64_t weight;
    	};
    	std::vector<block_stat> block_weights(start_num_blocks);
    	size_t cur_block = 0;
    	for(size_t i=0;i<fstats.dict_usage.size();i+=t_block_size_bytes) {
    		size_t max_usage = 0;
    		size_t block_size = fstats.dict_usage.size() - i;
    		if(block_size > t_block_size_bytes) block_size = t_block_size_bytes;
    		for(size_t j=0;j<block_size;j++) {
    			size_t usage = fstats.dict_usage[i+j];
    			max_usage = std::max(usage,max_usage);
    		}
    		block_weights[cur_block].weight = max_usage;
    		block_weights[cur_block++].offset = i;
    	}
    	/* (3) find the smallest blocks we want to remove */
    	LOG(INFO) << "\t" << "Find the smallest blocks we want to remove.";
    	auto bytes_to_remove = start_dict_size_bytes - target_dict_size_bytes;
    	auto num_blocks_to_remove = bytes_to_remove / t_block_size_bytes;
    	LOG(INFO) << "\t" << "Removing " << num_blocks_to_remove << " ("<< bytes_to_remove << " bytes)";
    	std::sort(block_weights.begin(),block_weights.end(),[](block_stat& a,block_stat& b) { return a.weight < b.weight; });
    	block_weights.resize(num_blocks_to_remove);
    	LOG(INFO) << "\t" << "Smallest block weight removed: " << block_weights[0].weight;
    	LOG(INFO) << "\t" << "Largest block weight removed: " << block_weights.back().weight;

    	/* (4) create the pruned dictionary */
    	LOG(INFO) << "\t" << "Create the pruned dictionary.";
    	std::sort(block_weights.begin(),block_weights.end(),[](block_stat& a,block_stat& b) { return a.offset < b.offset; });
    	{
    		const sdsl::int_vector_mapper<8, std::ios_base::in> dict(col.file_map[KEY_DICT]);
    		auto wdict = sdsl::write_out_buffer<8>::create(new_dict_file);
    		auto cur_block = 0;
    		for(size_t i=0;i<dict.size()-2;) {
    			if(block_weights[cur_block].offset == i) {
    				/* skip the block */
    				cur_block++;
    				i += t_block_size_bytes;
    			} else {
    				wdict.push_back(dict[i]);
    				i++;
    			}
    		}
    		wdict.push_back(0);
    		LOG(INFO) << "\t" << "Pruned dictionary size = " << wdict.size() / (1024*1024) << " MiB";
    	}
		col.file_map[KEY_DICT] = new_dict_file;
        auto end_total = hrclock::now();
        LOG(INFO) <<"\n" << "\t" << type() + " Total time = " << duration_cast<milliseconds>(end_total-start_total).count() / 1000.0f << " sec";
		col.compute_dict_hash();
    }
};
