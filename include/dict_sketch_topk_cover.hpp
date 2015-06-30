#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "count_min_sketch.hpp"
#include "chunk_freq_estimator.hpp"
#include "set_cover.hpp"

#include <unordered_set>

using namespace std::chrono;

template <
uint32_t t_block_size = 1024,
uint32_t t_estimator_block_size = 16,
uint32_t t_k = 50000
>
class dict_sketch_topk_cover {
public:
    static std::string type()
    {
        return "dict_sketch_topk_cover-"+ std::to_string(t_block_size)+"-"+ std::to_string(t_estimator_block_size)+"-"+ std::to_string(t_k);
    }

    static std::string file_name(collection& col, uint64_t size_in_bytes)
    {
        auto size_in_mb = size_in_bytes / (1024 * 1024);
        return col.path + "/index/" + type() + "-" + std::to_string(size_in_mb) + ".sdsl";
    }
public:
	static void create(collection& col, bool rebuild,size_t size_in_bytes) {
		uint32_t budget_bytes = size_in_bytes;
		uint32_t budget_mb = size_in_bytes / (1024 * 1024);
		uint32_t num_blocks_required = (budget_bytes / t_block_size) + 1;
        // check if we store it already and load it
        auto fname = file_name(col, size_in_bytes);
        col.file_map[KEY_DICT] = fname;
		if (! utils::file_exists(fname) || rebuild ) {  // construct
			using sketch_type = count_min_sketch<std::ratio<1, 3000000>,std::ratio<1, 10>>;
			chunk_freq_estimator_topk<t_estimator_block_size,t_k,sketch_type> cfe_topk;

			// (1) create frequency estimates
			// try to load the estimates instead of recomputing
			auto sketch_name = file_name(col,size_in_bytes) + "-sketch-" + cfe_topk.type();
			if (! utils::file_exists(sketch_name) || rebuild ) {
				LOG(INFO) << "\t" << "Create dictionary with budget " << budget_mb << " MiB";
				LOG(INFO) << "\t" << "Block size = " << t_block_size; 
				LOG(INFO) << "\t" << "Num blocks = " << num_blocks_required;
				{
					LOG(INFO) << "\t" << "Sketch size = " << cfe_topk.sketch.size_in_bytes()/(1024*1024) << " MiB";
					LOG(INFO) << "\t" << "Sketch params = {d=" << cfe_topk.sketch.d << ",w=" << cfe_topk.sketch.w << "}";
					LOG(INFO) << "\t" << "Estimating block frequencies";
					auto est_start = hrclock::now();
			        sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
					for(size_t i=0;i<text.size();i++) {
						auto sym = text[i];
						cfe_topk.update(sym);
					}
					auto est_stop = hrclock::now();
					auto est_error = cfe_topk.sketch.estimation_error();
					LOG(INFO) << "\t" << "Estimation time = " << duration_cast<milliseconds>(est_stop-est_start).count() / 1000.0f << " sec";
					LOG(INFO) << "\t" << "Sketch estimation error = " << est_error;
					LOG(INFO) << "\t" << "Sketch estimation confidence = " << cfe_topk.sketch.estimation_probability();
					LOG(INFO) << "\t" << "Sketch noise estimate = " << cfe_topk.sketch.noise_estimate();
					LOG(INFO) << "\t" << "Number of things hashed = " << cfe_topk.sketch.sketch.total_count;
					sdsl::store_to_file(cfe_topk,sketch_name);
				}
			} else {
				LOG(INFO) << "\t" << "Load sketch from file " << sketch_name;
				sdsl::load_from_file(cfe_topk,sketch_name);
				LOG(INFO) << "\t" << "Sketch estimation error = " << cfe_topk.sketch.estimation_error();
				LOG(INFO) << "\t" << "Sketch estimation confidence = " << cfe_topk.sketch.estimation_probability();
				LOG(INFO) << "\t" << "Sketch noise estimate = " << cfe_topk.sketch.noise_estimate();
				LOG(INFO) << "\t" << "Number of things hashed = " << cfe_topk.sketch.sketch.total_count;
			}


			// (2) identify all the blocks which include an item in the top-k 
			LOG(INFO) << "\t" << "Identify all blocks in T containing top-k items";
			uint64_t num_blocks_in_dict = 0;
			struct block_cover {
				uint32_t id;
				std::vector<uint32_t> contents;
				uint64_t weight() const {
					return contents.size();
				}
			};
			cover_pq<block_cover> topk_pq;

			class uint32_vector_hasher {
			public:
			  std::size_t operator()(std::vector<uint32_t> const& vec) const {
			    std::size_t ret = 0;
			    for(auto& i : vec) {
			      ret ^= std::hash<uint32_t>()(i);
			    }
			    return ret;
			  }
			};
			
			uint32_vector_hasher vec_hasher;
			{
				std::unordered_map<uint64_t,uint64_t> dict_blocks;
				{
					auto topk_items = cfe_topk.topk();
					size_t i=0;
					for(const auto& item : topk_items) {
						dict_blocks[item.second.hash] = i++;
					}

					LOG(INFO) << "\t" << "Threshold = " << topk_items.back().first;
				}
				num_blocks_in_dict = dict_blocks.size();
				block_cover cur_block;
				fixed_hasher<t_estimator_block_size> rk;
			    sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
			    std::unordered_set<uint64_t> included_hashes;
				for(size_t i=0;i<text.size();i+=t_block_size) {
					size_t block_size = std::min( (size_t)text.size() - i , (size_t)t_block_size);
					for(size_t j=0;j<block_size;j++) {
						auto sym = text[i+j];
						auto hash = rk.update(sym);
						if(j<t_estimator_block_size-1) continue;
						auto ditr = dict_blocks.find(hash);
						if( ditr != dict_blocks.end() ) {
							auto cid = ditr->second;
							cur_block.contents.push_back(cid);
						}
					}
					if(cur_block.contents.size() > 1) {
						// current block contains more than one top-k items!
						// make unique
						std::sort(cur_block.contents.begin(),cur_block.contents.end());
						auto last = std::unique(cur_block.contents.begin(),cur_block.contents.end());
						cur_block.contents.erase(last,cur_block.contents.end());
						cur_block.id = i;
						if(cur_block.contents.size() > 1) {
							auto hash = vec_hasher(cur_block.contents);
							if( included_hashes.count(hash) == 0 ) {
								topk_pq.push(cur_block);
								included_hashes.insert(hash);
							}
                        }
					}
                    cur_block.contents.clear();
				}
			}
			LOG(INFO) << "\t" << "Perform set cover";
			std::vector<uint64_t> picked_blocks;
			{
				LOG(INFO) << "topk_pq.size() = " << topk_pq.size();

				/* (2) perform the cover */
				sdsl::bit_vector covered(num_blocks_in_dict);
				uint64_t need_to_cover = num_blocks_in_dict;
				while(need_to_cover != 0 && ! topk_pq.empty() && picked_blocks.size() < num_blocks_required ) {
					/* (3) get the largest set */
					auto& most_uncovered_block = topk_pq.top();
					//LOG(INFO) << "top = " << most_uncovered_block.id << " w=" << most_uncovered_block.weight();
					/* (4) check if the count is correct */
					bool needed_update = false;
					auto itr = most_uncovered_block.contents.begin();
					while(itr != most_uncovered_block.contents.end()) {
						auto id = *itr;
						if( covered[id] == 1 ) {
							itr = most_uncovered_block.contents.erase(itr);
							needed_update = true;
						} else {
							itr++;
						}
					}
					//LOG(INFO) << "needs update = " << needed_update;
					if(needed_update) {
						//LOG(INFO) << "insert back to pq with weight " << most_uncovered_block.weight();
						if(most_uncovered_block.weight() > 0) topk_pq.sift_top();
						else topk_pq.pop();
					} else {
						/* add to picked blocks */
						//LOG(INFO) << "add to picked_blocks";
						picked_blocks.push_back(most_uncovered_block.id);
						need_to_cover -= most_uncovered_block.contents.size();
						for(const auto& id : most_uncovered_block.contents) {
							covered[id] = 1;
						}
						topk_pq.pop();
					}
				}
				//LOG(INFO) << "need_to_cover = " << need_to_cover;
			}
			std::sort(picked_blocks.begin(),picked_blocks.end());
			LOG(INFO) << "picked blocks = " << picked_blocks;
			LOG(INFO) << "\t" << "Writing dictionary"; 
			auto dict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
            {
			    sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
			    for(const auto& block_offset : picked_blocks) {
				    auto beg = text.begin()+block_offset;
				    auto end = beg + t_block_size;
				    std::copy(beg,end,std::back_inserter(dict));
			    }
            }
			dict.push_back(0);
			LOG(INFO) << "\t" << "Wrote " << dict.size() << " bytes";
		} else {
			LOG(INFO) << "\t" << "Dictionary exists at '" << fname << "'";
		}
		// compute a hash of the dict so we don't reconstruct things
		// later when we don't have to.
		col.compute_dict_hash();
	}
};
