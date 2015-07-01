#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "count_min_sketch.hpp"
#include "chunk_freq_estimator.hpp"

#include <unordered_set>

using namespace std::chrono;

template <
uint32_t t_block_size = 1024,
uint32_t t_estimator_block_size = 16
>
class dict_global_coverage_disjoint {
public:
    static std::string type()
    {
        return "dict_global_coverage_disjoint-"+ std::to_string(t_block_size)+"-"+ std::to_string(t_estimator_block_size);
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
		uint32_t num_blocks_required = (budget_bytes / t_estimator_block_size) + 1;

        // check if we store it already and load it
        auto fname = file_name(col, size_in_bytes);
        col.file_map[KEY_DICT] = fname;
		if (! utils::file_exists(fname) || rebuild ) {  // construct
			//double threshold = 0.0f;
			using sketch_type = count_min_sketch<std::ratio<1, 3000000>,std::ratio<1, 10>>;
			//chunk_freq_estimator_topk<16,500000,sketch_type> cfe_topk;
			chunk_freq_estimator<t_estimator_block_size,sketch_type> cfe; //build sketches cfe

			LOG(INFO) << "\t" << "Create dictionary with budget " << budget_mb << " MiB";
			LOG(INFO) << "\t" << "Block size = " << t_block_size; 
			LOG(INFO) << "\t" << "Num blocks = " << num_blocks_required; 

			auto num_samples = budget_bytes / t_block_size;
			auto num_subblocks = t_block_size / t_estimator_block_size;
            LOG(INFO) << "\tDictionary samples = " << num_samples;
            LOG(INFO) << "\tNum subblocks = " << num_subblocks;
            sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);

			// (1) create frequency estimates
			{
				LOG(INFO) << "\t" << "Sketch size = " << cfe.sketch.size_in_bytes()/(1024*1024) << " MiB";
				LOG(INFO) << "\t" << "Sketch params = {d=" << cfe.sketch.d << ",w=" << cfe.sketch.w << "}";
				LOG(INFO) << "\t" << "Estimating block frequencies";
				auto est_start = hrclock::now();
				for(size_t i=0;i<text.size();i++) {
					auto sym = text[i];
					cfe.update(sym);
				}
				auto est_stop = hrclock::now();
				auto est_error = cfe.sketch.estimation_error();
				LOG(INFO) << "\t" << "Estimation time = " << duration_cast<milliseconds>(est_stop-est_start).count() / 1000.0f << " sec";
				LOG(INFO) << "\t" << "Sketch estimation error = " << est_error;
				LOG(INFO) << "\t" << "Sketch estimation confidence = " << cfe.sketch.estimation_probability();
				//auto topk_blocks = cfe_topk.topk();
				//threshold = topk_blocks.back().first;
				//LOG(INFO) << "\t" << "Threshold freq = " << threshold;
			}

			// (2) compute uniform max coverage with sketches and write dict 
			rabin_karp_hasher<t_estimator_block_size> rk;
			for(size_t i=0;i<t_estimator_block_size-1;i++) rk.update(text[i]); // init RKH
				
			uint64_t sum_weights = 0;	
			uint32_t block_no = 0;

			struct block_cover {
				uint32_t id;
				uint64_t weight;
				sdsl::bit_vector bitmask; 
				block_cover(uint32_t size) {
					bitmask.resize(size);
				}
				bool operator<(const block_cover& bc) const {
					return weight < bc.weight;
				}
				bool operator>(const block_cover& bc) const {
					return weight > bc.weight;
				}
				bool operator==(const block_cover& bc) const {
					return bc.id == id;
				}
			};
			using boost_heap = boost::heap::fibonacci_heap<block_cover, boost::heap::compare<std::less<block_cover>>>;
			boost_heap cover_pq;
			std::unordered_map<uint64_t,uint64_t> small_blocks;
			size_t j=0; //index to bit vector map

			//First pass, build pq
			//note the code might have the non-divisible issue, memeroy issue is block size are small

			for(size_t i=t_estimator_block_size-1;i<text.size();i++) { //the actual max-cov computation
				auto sym = text[i];
				auto hash = rk.update(sym);

				//dealing with disjoint sub-blocks, rolling after the first block
				if((i+1)%t_estimator_block_size == 0) {
					auto est_freq = cfe.estimate(hash);
					sum_weights += est_freq;
					if(small_blocks.find(hash) == small_blocks.end()) {
						small_blocks[hash] = j++;
					} 

					if((i+1)%t_block_size == 0) {
						block_cover cov(num_subblocks);
						cov.id = block_no;
						cov.weight = sum_weights;
						cover_pq.push(cov);
						//update
						block_no++;
						sum_weights = 0;
					}
				}
			}

			//second pass, do maximum coverage, simulating the lazy approach for set-cover							
			LOG(INFO) << "\t" << "Perform maximum coverage";
			std::vector<uint64_t> picked_blocks;
			{
				LOG(INFO) << "cover_pq.size() = " << cover_pq.size();
				LOG(INFO) << "small_blocks.size() = " << small_blocks.size();
 
				sdsl::bit_vector covered(small_blocks.size()); //ordered
				uint32_t need_to_cover = small_blocks.size();

				while(need_to_cover > 0 && ! cover_pq.empty() && num_samples > 0) {
					//get the most weighted block
					auto most_weighted_block = cover_pq.top(); cover_pq.pop();
					//check if the weight order is correct
					bool needed_update = false;
					std::vector<uint64_t> hashes; //store small block hashes in order
					uint32_t content_uncovered = 0;
					for (size_t i = 0; i < num_subblocks;i++) {
						if(!most_weighted_block.bitmask[i]) {
							auto id = most_weighted_block.id * num_subblocks + i;
							auto hash = rk.compute_hash(text.begin() + id * t_estimator_block_size);
							hashes.push_back(hash);
							content_uncovered++;
						} else {
							hashes.push_back(2147483648ULL);//placeholder for covered small blocks
						}
					}

					for (size_t i = 0; i < hashes.size();i++) {
						auto hash = hashes[i];
						if(hash == 2147483648ULL) continue;

						if( covered[small_blocks[hash]] == 1 ) {
							//update weight
							auto est_freq = cfe.estimate(hash);
							most_weighted_block.weight -= est_freq;
							most_weighted_block.bitmask[i] = 1;
							content_uncovered--;
							needed_update = true;
						}
					}
					// LOG(INFO) << "\t" << most_weighted_block.bitmask;
					if(needed_update) {
						/* needed update */
						//LOG(INFO) << "\t" << "Needed Update!";
						cover_pq.push(most_weighted_block);
					} else {
						/* add to picked blocks */				
						picked_blocks.push_back(most_weighted_block.id);
						need_to_cover -= content_uncovered;
						for(const auto& hash : hashes) {
							if(hash != 2147483648ULL)
								covered[small_blocks[hash]] = 1;
						}
						num_samples--;
						// LOG(INFO) << "\t" << "Blocks left to pick: " << num_samples;
						LOG(INFO) << "\t" << "Blocks weight: " << most_weighted_block.weight;
					}
				}
				LOG(INFO) << "\t" << "Left to cover: " << need_to_cover;
				LOG(INFO) << "\t" << "Covered in heap: " << text.size()/t_block_size-cover_pq.size();
			}
			std::sort(picked_blocks.begin(),picked_blocks.end());
			LOG(INFO) << "picked blocks = " << picked_blocks;
			LOG(INFO) << "\t" << "Writing dictionary"; 
			auto dict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
            {
			    sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
			    for(const auto& block_offset : picked_blocks) {
				    auto beg = text.begin()+block_offset*t_block_size;
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
