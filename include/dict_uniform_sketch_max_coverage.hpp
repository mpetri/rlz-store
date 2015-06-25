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
class dict_uniform_sketch_max_coverage {
public:
    static std::string type()
    {
        return "dict_uniform_sketch_max_coverage-"+ std::to_string(t_block_size)+"-"+ std::to_string(t_estimator_block_size);
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
			chunk_freq_estimator<16,sketch_type> cfe; //build sketches cfe

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
			LOG(INFO) << "\t" << "Writing dictionary"; 
			auto dict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
			size_t dict_written = 0;

			std::unordered_set<uint64_t> dict_blocks;
			//size_t blocks_found = 0; size_t len = 0;
			rabin_karp_hasher<t_estimator_block_size> rk;
			for(size_t i=0;i<t_estimator_block_size-1;i++) rk.update(text[i]); // init RKH
				
			uint64_t sum_weights_max = std::numeric_limits<uint64_t>::min();
			uint64_t sum_weights_current = 0;	
			uint32_t block_no = 0;
			uint32_t best_block_idx = 0;

			uint32_t skip_size = 0;
			std::unordered_set<uint64_t> step_blocks;

			//assumsing that sub_blocksize is smaller
			for(size_t i=t_estimator_block_size-1;i<text.size();i++) { //the actual max-cov computation
				auto sym = text[i];
				auto hash = rk.update(sym);

				//dealing with disjoint sub-blocks, rolling after the first block
				if((i+1)%t_estimator_block_size == 0) {
					auto est_freq = cfe.estimate(hash);

					if(skip_size < t_block_size) {
						skip_size+=t_estimator_block_size;				
						sum_weights_current += est_freq;
					}
					else {//start rolling until a step is reached
						sum_weights_current -= rk.compute_hash(text.begin() + (block_no - 1) * t_estimator_block_size);
						if(step_blocks.find(hash) == step_blocks.end())
							sum_weights_current += est_freq;

						if(sum_weights_current >= sum_weights_max)
						{
							sum_weights_max = sum_weights_current;
							best_block_idx = block_no;
						}
						block_no++;
					}

					//passing the first block of each step_block
					if(skip_size == t_block_size) {
						if (sum_weights_current >= sum_weights_max)
							sum_weights_max = sum_weights_current;
						skip_size++; //make skip_size not valid to start rolling 
						block_no++;
					}

					//dealing with sample_steps, pick the best and write to step_blocks and dic, resetting
					if((i+1)%sample_step == 0) { 
						auto start = text.begin() + best_block_idx * t_estimator_block_size;
						auto end = start + t_block_size;
						uint32_t k = 0;
						
						while(k < t_block_size / t_estimator_block_size) {
							step_blocks.insert(rk.compute_hash(start + k * t_estimator_block_size));
							k++;
						}
						std::copy(start,end,std::back_inserter(dict));
						dict_written += t_block_size;
						LOG(INFO) << "\t" << "Dic written = " << dict_written << " kB"; 

						//resetting for the next step
						sum_weights_max = std::numeric_limits<uint64_t>::min();
						sum_weights_current = 0;
						skip_size = 0;
					}
				}
				// if(dict_written > budget_bytes) {
				// 	LOG(INFO) << "\t" << "Text scanned = " << 100*(float)i / (float)text.size();
				// 	break;
				// }
				//
			}

			//LOG(INFO) << "\t" << "Blocks found = " << blocks_found;
			LOG(INFO) << "\t" << "Final dictionary size = " << dict_written/(1024*1024) << " MiB"; 
			dict.push_back(0); // zero terminate for SA construction

		} else {
			LOG(INFO) << "\t" << "Dictionary exists at '" << fname << "'";
		}
		// compute a hash of the dict so we don't reconstruct things
		// later when we don't have to.
		col.compute_dict_hash();
	}
};
