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
uint32_t t_estimator_block_size = 8,
uint32_t t_start_threshold = 16,
uint32_t t_max_cover_size = 50000000
>
class dict_sketch_topk_cover {
public:
    static std::string type()
    {
        return "dict_sketch_topk_cover-"+ std::to_string(t_block_size)+"-"+
         std::to_string(t_estimator_block_size)+"-"+ std::to_string(t_start_threshold)
         +"-"+ std::to_string(t_max_cover_size);
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
			LOG(INFO) << "\t" << "Create dictionary with budget " << budget_mb << " MiB";
			using sketch_type = count_min_sketch<std::ratio<1,20000000>,std::ratio<1, 20>>;
			using hasher_type = fixed_hasher<t_estimator_block_size>;
			using cfe_type = chunk_freq_estimator<t_estimator_block_size,hasher_type,sketch_type>;
			// (1) create frequency estimates
			// try to load the estimates instead of recomputing
			cfe_type cfe;
			LOG(INFO) << "\t" << "Sketch size = " << cfe.sketch.size_in_bytes()/(1024*1024) << " MiB";
			auto sketch_name = file_name(col,size_in_bytes) + "-sketch-" + cfe_type::type();
			if (! utils::file_exists(sketch_name) || rebuild ) {
				LOG(INFO) << "\t" << "Building CM sketch";
				auto start = hrclock::now();
				sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
				cfe = cfe_type::parallel_sketch(text.begin(),text.end());
				auto stop = hrclock::now();
				LOG(INFO) << "\t" << "Estimation time = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";
				LOG(INFO) << "\t" << "Store sketch to file " << sketch_name;
				sdsl::store_to_file(cfe,sketch_name);
			} else {
				LOG(INFO) << "\t" << "Load sketch from file " << sketch_name;
				sdsl::load_from_file(cfe,sketch_name);
			}
			LOG(INFO) << "\t" << "Sketch params = {d=" << cfe.sketch.d << ",w=" << cfe.sketch.w << "}";
			LOG(INFO) << "\t" << "Sketch estimation error = " << cfe.sketch.estimation_error();
			LOG(INFO) << "\t" << "Sketch estimation confidence = " << cfe.sketch.estimation_probability();
			LOG(INFO) << "\t" << "Sketch noise estimate = " << cfe.sketch.noise_estimate();
			LOG(INFO) << "\t" << "Number of things hashed = " << cfe.sketch.total_count();


			// (2) identify all the blocks which include an item in the top-k 
			LOG(INFO) << "\t" << "Identify all blocks in T containing top-k items";
			struct block_cover {
				uint32_t id;
				std::vector<uint32_t> contents;
				uint64_t weight() const {
					return contents.size();
				}
			};
			LOG(INFO) << "\t" << "Start Threshold = " << t_start_threshold;
			auto threshold = t_start_threshold;
			sdsl::bit_vector covered;
			cover_pq<block_cover> topk_pq;
			size_t blocks_non_freq = 0;
			{
				hasher_type rk;
			    sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
			    std::unordered_map<uint64_t,uint64_t> dict_blocks;
			    size_t j = 0;
				for(size_t i=0;i<text.size();i+=t_block_size) {
					size_t block_size = std::min( (size_t)text.size() - i , (size_t)t_block_size);
					block_cover cur_block;
					for(size_t j=0;j<block_size;j++) {
						auto sym = text[i+j];
						uint64_t hash = rk.update(sym);
						if(j<t_estimator_block_size-1) continue;
						auto est = cfe.estimate(hash);
						if( est >= threshold ) {
							uint64_t new_id = dict_blocks.size();
							auto pditr = dict_blocks.emplace(hash,new_id);
							auto ditr = pditr.first;
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
							topk_pq.push(cur_block);
                        } else {
                        	blocks_non_freq++;
                        }
					} else {
						blocks_non_freq++;
					}

					if(j % 1000000 == 0) {
						LOG(INFO) << "pq stats after " << i << " syms";
						topk_pq.print_stats();
						LOG(INFO) << "total blocks = " << j+1;
						LOG(INFO) << "blocks containing no frequent chunks = " << blocks_non_freq;
						LOG(INFO) << "non freq percent = " << 100.0*(double)blocks_non_freq / (double)(j+1);
						LOG(INFO) << "dict_blocks size = " <<  dict_blocks.size();
					}

					j++;
				}
				covered.resize(dict_blocks.size());
			}
			LOG(INFO) << "\t" << "Perform set cover";
			std::vector<uint64_t> picked_blocks;
			{
				LOG(INFO) << "topk_pq.size() = " << topk_pq.size();
				/* (2) perform the cover */
				uint64_t need_to_cover = covered.size();
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
