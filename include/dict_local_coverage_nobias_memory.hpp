#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "count_min_sketch.hpp"
#include "chunk_freq_estimator.hpp"
#include "set_cover.hpp"

#include <unordered_set>
#include <queue>

using namespace std::chrono;

template <
uint32_t t_block_size = 1024,
uint32_t t_estimator_block_size = 16
>
class dict_local_coverage_nobias_memory {
public:
    static std::string type()
    {
        return "dict_local_coverage_nobias_memory-"+ std::to_string(t_block_size)+"-"+ std::to_string(t_estimator_block_size);
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
			// using sketch_type = count_min_sketch<std::ratio<1, 2000000>,std::ratio<1, 5>>;
			using sketch_type = count_min_sketch<std::ratio<1, 3000000>,std::ratio<1, 10>>; //for 1gb
			// using sketch_type = count_min_sketch<std::ratio<1, 6000000>,std::ratio<1, 10>>; //for 2gb
			// using sketch_type = count_min_sketch<std::ratio<1, 30000000>,std::ratio<1, 20>>; //for 10gb
			using hasher_type = fixed_hasher<t_estimator_block_size>;
			using cfe_type = chunk_freq_estimator<t_estimator_block_size,hasher_type,sketch_type>;
			cfe_type cfe;
			// chunk_freq_estimator_topk<16,500000,sketch_type> cfe_topk;
			// chunk_freq_estimator<t_estimator_block_size, sketch_type> cfe; //build sketches cfe

			LOG(INFO) << "\t" << "Create dictionary with budget " << budget_mb << " MiB";
			LOG(INFO) << "\t" << "Block size = " << t_block_size; 
			LOG(INFO) << "\t" << "Num blocks = " << num_blocks_required; 

			sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
			auto num_samples = budget_bytes / t_block_size;
            LOG(INFO) << "\tDictionary samples = " << num_samples;
            auto n = text.size();
            size_t sample_step = n / num_samples;
            LOG(INFO) << "\tSample steps = " << sample_step;

			// (1) create frequency estimates
			// try to load the estimates instead of recomputing
			LOG(INFO) << "\t" << "Sketch size = " << cfe.sketch.size_in_bytes()/(1024*1024) << " MiB";
			auto sketch_name = file_name(col,size_in_bytes) + "-sketch-" + cfe.type();
			if (! utils::file_exists(sketch_name) || rebuild ) {		
				LOG(INFO) << "\t" << "Building CM sketch";
				auto start = hrclock::now();
				sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
				cfe = cfe_type::parallel_sketch(text.begin(),text.end(),3);
				auto stop = hrclock::now();
				LOG(INFO) << "\t" << "Estimation time = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";
				LOG(INFO) << "\t" << "Store sketch to file " << sketch_name;
				sdsl::store_to_file(cfe,sketch_name);
			} else {
				LOG(INFO) << "\t" << "Load sketch from file " << sketch_name;
				sdsl::load_from_file(cfe,sketch_name);
				// LOG(INFO) << "\t" << "Maximum frequency = " << cfe.max_freq();
				// LOG(INFO) << "\t" << "Number of things hashed = " << cfe.sketch.sketch.total_count;
			}
			double cfe_noise = cfe.sketch.noise_estimate();		
			double cfe_error = cfe.sketch.estimation_error()*0.2;
			LOG(INFO) << "\t" << "Sketch params = {d=" << cfe.sketch.d << ",w=" << cfe.sketch.w << "}";
			LOG(INFO) << "\t" << "Sketch estimation error = " << cfe.sketch.estimation_error();
			LOG(INFO) << "\t" << "Sketch estimation confidence = " << cfe.sketch.estimation_probability();
			LOG(INFO) << "\t" << "Sketch noise estimate = " << cfe_noise;
			LOG(INFO) << "\t" << "Number of things hashed = " << cfe.sketch.total_count();


			// (2) compute uniform max coverage with sketches and write dict 
			auto start = hrclock::now();
			fixed_hasher<t_estimator_block_size> rk;

			struct block_cover {
				uint32_t block_id;
				uint32_t step_id;
				uint64_t val;
				std::unordered_set<uint64_t> contents;

				uint64_t weight() const {
					return val;
				}
				bool operator<(const block_cover& bc) const {
					return val < bc.val;
				}
				bool operator>(const block_cover& bc) const {
					return val > bc.val;
				}
				bool operator==(const block_cover& bc) const {
					return (bc.step_id == step_id) && (bc.block_id == block_id);
				}
			};

			// using boost_heap = boost::heap::fibonacci_heap<block_cover, boost::heap::compare<std::less<block_cover>>>;
			// boost_heap c_pq;
			using heap = std::priority_queue<block_cover, std::vector<block_cover>, std::less<block_cover> >;
			heap c_pq;
			// cover_pq<block_cover> c_pq;
			std::unordered_map<uint64_t,uint32_t> small_blocks;

			size_t h=0; //index to bit vector map
			// uint32_t threshold = 1000;//change it later to quantile threshold as a parameter, simulating top k

			//1st pass, build pq
			LOG(INFO) << "\t" << "1st pass: constructing heap...";
			//note the code might have the non-divisible issue, memeroy issue is block size are small
			for(size_t i=0;i<text.size();i = i+sample_step) {
				for(size_t j=0;j<sample_step;j = j+t_block_size) { 
					std::unordered_set<uint64_t> local_blocks;
					uint64_t sum_weights = 0;
					
					for(size_t k=0;k<t_block_size;k++) {
						auto sym = text[i+j+k];
						auto hash = rk.update(sym);
						if(j+k < t_estimator_block_size-1) continue;

						auto est_freq = cfe.estimate(hash);
						if(est_freq >= cfe_error && local_blocks.find(hash) == local_blocks.end()) {
							local_blocks.emplace(hash);
							sum_weights += est_freq;
							//build global binary cover indices
							if(small_blocks.find(hash) == small_blocks.end())
								small_blocks[hash] = h++;
						}
					}
					block_cover cov;
					cov.step_id = i/sample_step;
					cov.block_id = j/t_block_size;
					cov.val = sum_weights;
					cov.contents = local_blocks;
					c_pq.push(cov);	
				}
			}	
			auto stop = hrclock::now();
			LOG(INFO) << "\t" << "1st pass runtime = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";
			LOG(INFO) << "\t" << "Distinct valid small blocks = " << small_blocks.size();

			//2nd pass, do maximum coverage, simulating the lazy approach for set-cover							
			LOG(INFO) << "\t" << "2nd pass: perform maximum coverage...";
			start = hrclock::now();
			std::vector<uint64_t> picked_blocks;
			{
				LOG(INFO) << "big block heap size = " << c_pq.size();
				LOG(INFO) << "small blocks to cover size = " << small_blocks.size();
 
				sdsl::bit_vector covered(small_blocks.size());
				sdsl::bit_vector step_pos(num_samples); //ordered step positions
				uint64_t need_to_cover = small_blocks.size();

				while(need_to_cover > 0 && ! c_pq.empty() && num_samples > 0) {
					//get the most weighted block
					auto most_weighted_block = c_pq.top(); //aiming at the top one that not in a covered step
					//check if the weight order is correct
					bool needed_update = false;
					while(step_pos[most_weighted_block.step_id] == 1) {
						c_pq.pop();
						most_weighted_block = c_pq.top(); 
					}
					c_pq.pop();
					auto itr = most_weighted_block.contents.begin();
					while(itr != most_weighted_block.contents.end()) {
						auto hash = *itr;
						if( covered[small_blocks[hash]] == 1 ) {			
							most_weighted_block.val -= cfe.estimate(hash);
							needed_update = true;
							itr = most_weighted_block.contents.erase(itr);
						} else {
							itr++;
						}
					}
				
					if(needed_update) {
						/* needed update */
						//LOG(INFO) << "\t" << "Needed Update!";
						if(most_weighted_block.contents.size() > 0) c_pq.push(most_weighted_block);
					} else {
						/* add to picked blocks */				
						step_pos[most_weighted_block.step_id] = 1; //set this step
						picked_blocks.push_back(most_weighted_block.step_id * sample_step + most_weighted_block.block_id * t_block_size);
						need_to_cover -= most_weighted_block.contents.size();
						for(const auto& hash : most_weighted_block.contents) {
							covered[small_blocks[hash]] = 1;
						}
						num_samples--;
						//c_pq.pop();
						// LOG(INFO) << "\t" << "Blocks left to pick: " << num_samples;
						LOG(INFO) << "\t" << "Blocks weight: " << most_weighted_block.val;
					}
				}
				// LOG(INFO) << "\t" << "Covered small blocks: " << small_blocks.size() - need_to_cover;
				// LOG(INFO) << "\t" << "Covered in heap: " << text.size()/t_block_size-c_pq.size();
			}
			stop = hrclock::now();
			LOG(INFO) << "\t" << "2nd pass runtime = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";
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
