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
class dict_local_coverage_nobias_sorted{
public:
    static std::string type()
    {
        return "dict_local_coverage_nobias_sorted-"+ std::to_string(t_block_size)+"-"+ std::to_string(t_estimator_block_size);
    }

    static std::string file_name(collection& col, uint64_t size_in_bytes)
    {
        auto size_in_mb = size_in_bytes / (1024 * 1024);
        return col.path + "/index/" + type() + "-" + std::to_string(size_in_mb) + ".sdsl";
    }
public:
	static bool pairCompareSecond(const std::pair<uint32_t,uint64_t>& firstElem, const std::pair<uint32_t,uint64_t>& secondElem) {
		return firstElem.second < secondElem.second;
	}

	static bool pairCompareFirst(const std::pair<uint32_t,uint32_t>& firstElem, const std::pair<uint32_t,uint32_t>& secondElem) {
		return firstElem.first < secondElem.first;
	}

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
			// using sketch_type = count_min_sketch<std::ratio<1, 3000000>,std::ratio<1, 10>>; //for 1gb
			//  using sketch_type = count_min_sketch<std::ratio<1, 6000000>,std::ratio<1, 10>>; //for 2gb
			 using sketch_type = count_min_sketch<std::ratio<1, 20000000>,std::ratio<1, 10>>; //for 10gb
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
			double cfe_error = cfe.sketch.estimation_error()*0.1;
			LOG(INFO) << "\t" << "Sketch params = {d=" << cfe.sketch.d << ",w=" << cfe.sketch.w << "}";
			LOG(INFO) << "\t" << "Sketch estimation error = " << cfe.sketch.estimation_error();
			LOG(INFO) << "\t" << "Sketch estimation confidence = " << cfe.sketch.estimation_probability();
			LOG(INFO) << "\t" << "Sketch noise estimate = " << cfe_noise;
			LOG(INFO) << "\t" << "Number of things hashed = " << cfe.sketch.total_count();
			

			//first pass getting densest steps!
			std::vector<std::pair <uint32_t,uint64_t>> steps;
			fixed_hasher<t_estimator_block_size> rk;
			LOG(INFO) << "\t" << "First pass: getting the densest steps..."; 
			auto start = hrclock::now();
			// for(size_t i=0;i<text.size();i = i+sample_step) { 
			// 	uint64_t sum_weights_step = 0;	
			// 	std::unordered_set<uint64_t> step_blocks;
			// 	for(size_t j=0;j<sample_step;j++) { 				
			// 		auto sym = text[i+j];
			// 		auto hash = rk.update(sym);
			// 		if(j < t_estimator_block_size-1) continue;

			// 		if(step_blocks.find(hash) == step_blocks.end()) //continue rolling
			// 		{		
			// 			step_blocks.emplace(hash);				
			// 			auto est_freq = cfe.estimate(hash);
			// 			if(est_freq >= cfe_error)  //try a bit threshold pruning					
			// 				sum_weights_step += est_freq;
			// 		}
			// 	}
			// 	steps.push_back(std::make_pair(i/sample_step,sum_weights_step));	
			// }
			// //sort steps
			// std::sort(steps.begin(), steps.end(), pairCompareSecond);

			// //pull out the indices
			// std::vector<uint32_t> step_indices;
			// for(const auto& step : steps) {
			// 	step_indices.push_back(step.first);
		 //    }
		 //    steps.clear(); //save memory

			std::vector<uint32_t> step_indices;
			//try ordered max cov from front to back, proven to be good for small datasets
			for (size_t i = 0; i < num_samples; i++) {
				step_indices.push_back(i);
			}
			// //try randomly ordered max cov
			std::random_shuffle(step_indices.begin(), step_indices.end());
			
			// LOG(INFO) << "\t" << "Step indices size = " << step_indices.size(); 


		    // LOG(INFO) << "\t" << "ordered step indices = " << step_indices.size(); 
		    auto stop = hrclock::now();
		    LOG(INFO) << "\t" << "1st pass runtime = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";


			// 2nd pass: process max coverage using the sorted order by density
			// uint32_t threshold = 10000;//change it later to quantile threshold as a parameter, simulating top k
			std::unordered_set<uint64_t> step_blocks;
			std::vector<uint64_t> picked_blocks;
			// fixed_hasher<t_estimator_block_size> rk;
			LOG(INFO) << "\t" << "Second pass: perform ordered max coverage..."; 
			start = hrclock::now();

			for(const auto& i : step_indices) {//steps
				uint64_t sum_weights_max = std::numeric_limits<uint64_t>::min();
				uint64_t sum_weights_current = 0;	
				uint32_t best_block_no = 0;
				// fixed_hasher<t_estimator_block_size> rk;
				std::unordered_set<uint64_t> best_local_blocks;

				for(size_t j=0;j<sample_step;j = j+t_block_size) {//blocks 
					std::unordered_set<uint64_t> local_blocks;

					for(size_t k=0;k<t_block_size;k++) {//bytes
						auto sym = text[i*sample_step+j+k];
						auto hash = rk.update(sym);

						if(k < t_estimator_block_size-1) continue;

						if(step_blocks.find(hash) == step_blocks.end() && local_blocks.find(hash) == local_blocks.end()) //continues rolling
						{		
							local_blocks.emplace(hash);				
							auto est_freq = cfe.estimate(hash);
							if(est_freq >= cfe_error)  //try a bit threshold pruning					
								sum_weights_current += est_freq;
						}
					}
					
					if(sum_weights_current >= sum_weights_max)
					{
						sum_weights_max = sum_weights_current;
						best_block_no = j/t_block_size; 
						best_local_blocks = local_blocks;
					}
					sum_weights_current = 0;
				}

				step_blocks.insert(best_local_blocks.begin(), best_local_blocks.end());
				picked_blocks.push_back(i * sample_step + best_block_no * t_block_size);
		    }   
			LOG(INFO) << "\t" << "Blocks size to check = " << step_blocks.size(); 
		    step_blocks.clear(); //save mem
		    //sort picked blocks
			std::sort(picked_blocks.begin(), picked_blocks.end());
			stop = hrclock::now();
			LOG(INFO) << "\t" << "Picked blocks = " << picked_blocks; 
			LOG(INFO) << "\t" << "2nd pass runtime = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";

		    // last pass: writing to dict
			LOG(INFO) << "\t" << "Last: writing dictionary..."; 
			auto dict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
            {
			    sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
			    for(const auto& pb : picked_blocks) {
				    auto beg = text.begin() + pb;
				    auto end = beg + t_block_size;
				    std::copy(beg,end,std::back_inserter(dict));
			    }
            }
	
			LOG(INFO) << "\t" << "Final dictionary size = " << dict.size()/(1024*1024) << " MiB"; 
			dict.push_back(0); // zero terminate for SA construction
		} else {
			LOG(INFO) << "\t" << "Dictionary exists at '" << fname << "'";
		}
		// compute a hash of the dict so we don't reconstruct things
		// later when we don't have to.
		col.compute_dict_hash();
	}
};
