#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "count_min_sketch.hpp"
#include "chunk_freq_estimator.hpp"

#include <unordered_set>

using namespace std::chrono;

template <
uint32_t t_block_size = 1024,
uint32_t t_estimator_block_size = 16,
uint32_t t_down_size = 256,
uint32_t topk = 2
>
class dict_local_weighted_coverage_random_CMS_topk{
public:
    static std::string type()
    {
        return "dict_local_weighted_coverage_random_CMS_topk-"+ std::to_string(t_block_size)+"-"+ std::to_string(t_estimator_block_size);
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
			// using sketch_type = count_min_sketch<std::ratio<1, 100000>,std::ratio<1, 8>>; //for 1gb RS		
			using sketch_type = count_min_sketch<std::ratio<1, 1000000>,std::ratio<1, 8>>; //for 10gb RS 16gb RS
			// using sketch_type = count_min_sketch<std::ratio<1, 3000000>,std::ratio<1, 8>>; //for 1gb
			// using sketch_type = count_min_sketch<std::ratio<1, 300000000>,std::ratio<1, 8>>; //for 128gb
			// using sketch_type = count_min_sketch<std::ratio<1, 6000000>,std::ratio<1, 8>>; //for 2gb
			// using sketch_type = count_min_sketch<std::ratio<1, 20000000>,std::ratio<1, 8>>; //for 10gb
			using hasher_type = fixed_hasher<t_estimator_block_size>;
			using cfe_type = chunk_freq_estimator<t_estimator_block_size,hasher_type,sketch_type>;
			cfe_type cfe;

			LOG(INFO) << "\t" << "Create dictionary with budget " << budget_mb << " MiB";
			LOG(INFO) << "\t" << "Block size = " << t_block_size; 
			LOG(INFO) << "\t" << "Num blocks = " << num_blocks_required; 

			sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
			auto num_samples = budget_bytes / t_block_size;
            LOG(INFO) << "\tDictionary samples = " << num_samples;
            auto n = text.size();
            size_t sample_step = n / num_samples * topk;     
            size_t sample_step_adjusted = sample_step / t_block_size * t_block_size;
            size_t num_samples_adjusted = n / sample_step_adjusted; //may contain more samples

            LOG(INFO) << "\tSample steps = " << sample_step;
            LOG(INFO) << "\tAdjusted sample steps = " << sample_step_adjusted;
            LOG(INFO) << "\tAdjusted dictionary samples  = " << num_samples_adjusted;

			// (1) create frequency estimates
			// try to load the estimates instead of recomputing
			LOG(INFO) << "\t" << "Sketch size = " << cfe.sketch.size_in_bytes()/(1024*1024) << " MiB";
		   	// uint32_t down_size = 256;
			auto sketch_name = file_name(col,size_in_bytes) + "-RSsketch-" + std::to_string(t_down_size) + "-" + cfe.type();
			auto rs_name = file_name(col,size_in_bytes) + "-RSample-" +std::to_string(t_down_size);
			fixed_hasher<t_estimator_block_size> rk;

			uint64_t rs_size = (text.size()-t_estimator_block_size)/t_down_size;
			std::vector<uint64_t> rs; //filter out frequency less than 64
			if (! utils::file_exists(rs_name) || rebuild) {		
			auto start = hrclock::now();
				LOG(INFO) << "\t" << "Building Reservoir sample";
				//make a reservoir sampler and keep it in RAM
				uint64_t count = 0;
				uint64_t skip = 0;
				std::random_device rd;
				std::mt19937 gen(rd());
				std::uniform_real_distribution<double> dis(0, 1);

				//double r = dis(gen);
				double w = std::exp(std::log(dis(gen))/rs_size);
				double s = std::floor(std::log(dis(gen))/std::log(1-w));
			
				for(size_t i=0;i<text.size();i++) {
					auto sym = text[i];
					auto hash = rk.update(sym);

					if(i < t_estimator_block_size-1) continue;
					else {
						if(count < rs_size) 
							rs.push_back(hash);
						else {
							skip++;
						// std::cout.precision(20);
						// std::cout << "\t" << "w=" << w << " s=" << s;	
						//	std::uniform_int_distribution<uint64_t> dis(0, count);
						//	uint64_t pos = dis(gen);
						//	if(pos < rs_size)
						//		rs[pos] = hash;
							if(s + 1 <= text.size()-t_estimator_block_size) {
								if(skip == s + 1) {
									// LOG(INFO) << "\t" << "here: " << count;
									rs[1+std::floor(rs_size*dis(gen))] = hash;
									w *= std::exp(std::log(dis(gen))/rs_size);
									s = std::floor(std::log(dis(gen))/std::log(1-w));
									skip = 0;
								}
							}
							else
								break; 	
						}
						count++;
					}						
				}
				auto stop = hrclock::now();
				LOG(INFO) << "\t" << "Reservoir sampling time = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";
				LOG(INFO) << "\t" << "Store reservoir sample to file " << rs_name;
				// sdsl::store_to_file(rs,rs_name);

				auto rs_file = sdsl::write_out_buffer<64>::create(rs_name);
	            {
	            	auto itr = rs.begin();
	            	while(itr != rs.end()) {
						std::copy(itr, itr+1, std::back_inserter(rs_file));
						itr++;
					}
	            }
			} else {
				LOG(INFO) << "\t" << "Load reservoir sample from file " << rs_name;
				// sdsl::load_from_file(rs,sketch_name);
				sdsl::read_only_mapper<64> rs_file(rs_name);
				auto itr = rs_file.begin();
				while(itr != rs_file.end()) {
					rs.push_back(*itr);
					itr++;
				}
			}
			
			LOG(INFO) << "\t" << "Reservoir sample blocks = " << rs.size(); 	
			LOG(INFO) << "\t" << "Reservoir sample size = " << rs.size()*8/(1024*1024) << " MiB";

			//make a smaller CMS from RAM
			if (! utils::file_exists(sketch_name)  || rebuild) {	
				LOG(INFO) << "\t" << "Building CM sketch";
				auto start = hrclock::now();
				cfe = cfe_type::parallel_sketch(rs.begin(),rs.end(),7);
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
			
			//make a useful blocks hash, clear RA as you go to save space
			std::sort(rs.begin(), rs.end());
			auto last = std::unique(rs.begin(), rs.end());
    		rs.erase(last, rs.end());
    		// LOG(INFO) << "\t" << "RS size = " << rs.size(); 	
			std::unordered_set<uint64_t> useful_blocks;		
			useful_blocks.max_load_factor(0.3);

			std::move(rs.begin(), rs.end(), std::inserter(useful_blocks, useful_blocks.end()));
			LOG(INFO) << "\t" << "Useful kept small blocks no. = " << useful_blocks.size(); 	

			//first pass getting densest steps!
			// std::vector<std::pair <uint32_t,uint64_t>> steps;
			LOG(INFO) << "\t" << "First pass: getting random steps..."; 
			auto start = hrclock::now();

			std::vector<uint32_t> step_indices;
			//try ordered max cov from front to back, proven to be good for small datasets
			for (size_t i = 0; i < num_samples_adjusted; i++) {
				step_indices.push_back(i);
			}
			// //try randomly ordered max cov
			// std::random_shuffle(step_indices.begin(), step_indices.end());
			
		    auto stop = hrclock::now();
		    LOG(INFO) << "\t" << "1st pass runtime = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";

			// 2nd pass: process max coverage using the sorted order by density
			// uint32_t threshold = 10000;//change it later to quantile threshold as a parameter, simulating top k
			std::unordered_set<uint64_t> step_blocks;
			step_blocks.max_load_factor(0.2);
			std::vector<uint64_t> picked_blocks;
			LOG(INFO) << "\t" << "Second pass: perform ordered max coverage..."; 
			start = hrclock::now();
			struct block_cover {
				uint64_t block_pos;
				uint64_t val; //frequency or weights etc.
				std::unordered_set<uint64_t> contents;

				bool operator<(const block_cover& bc) const {
					return val < bc.val || (val == bc.val && block_pos > bc.block_pos);
				}
				bool operator>(const block_cover& bc) const {
					return val > bc.val || (val == bc.val && block_pos < bc.block_pos);
				}
				bool operator==(const block_cover& bc) const {
					return val == bc.val && block_pos == bc.block_pos;
				}
			};

			for(const auto& i : step_indices) {//steps
				// uint64_t sum_weights_max = std::numeric_limits<uint64_t>::min();	
				uint64_t step_pos = i*sample_step_adjusted;
				// uint64_t best_block_no = 0;
				// std::unordered_set<uint64_t> best_local_blocks;
				std::vector<block_cover> blocks;

				for(size_t j=0;j<sample_step_adjusted;j = j+t_block_size) {//blocks 
					std::unordered_set<uint64_t> local_blocks;
					local_blocks.max_load_factor(0.1);
					uint64_t sum_weights_current = 0;

					for(size_t k=0;k<t_block_size;k++) {//bytes
						auto sym = text[step_pos+j+k];
						auto hash = rk.update(sym);

						if(k < t_estimator_block_size-1) continue;

						if(local_blocks.find(hash) == local_blocks.end() && step_blocks.find(hash) == step_blocks.end() && useful_blocks.find(hash) != useful_blocks.end()) //continues rolling
						{//expensive checking					
							local_blocks.emplace(hash);
							auto est_freq = cfe.estimate(hash);
							if(est_freq >= cfe_error)						
								sum_weights_current += est_freq;		
						}
					}
					
					block_cover cov;
					cov.block_pos = step_pos + j;
					cov.val = sum_weights_current;
					cov.contents = std::move(local_blocks);
					blocks.push_back(cov);
				}

				std::sort(blocks.begin(), blocks.end(), std::greater<block_cover>());
				bool filled = false;
				for(size_t k=0;k<topk;k++) {
					picked_blocks.push_back(blocks[k].block_pos);
				
					// LOG(INFO) << "\t" << "Blocks picked: " << picked_blocks.size(); 
					if(picked_blocks.size() >= num_samples)  {
						filled = true;
						break;
					}		
					// LOG(INFO) << "\t" << "max weight = " << sum_weights_max;
					step_blocks.insert(blocks[k].contents.begin(), blocks[k].contents.end());
				}
				if(filled) break;
		    }   
			LOG(INFO) << "\t" << "Blocks size to check = " << step_blocks.size(); 
		    step_blocks.clear(); //save mem
		    step_indices.clear();//
		    useful_blocks.clear();
	
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
