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
class dict_sketch_topk_scan {
public:
    static std::string type()
    {
        return "dict_sketch_topk_scan-"+ std::to_string(t_block_size)+"-"+ std::to_string(t_estimator_block_size);
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
			double threshold = 0.0f;
			using sketch_type = count_min_sketch<std::ratio<1, 3000000>,std::ratio<1, 10>>;
			chunk_freq_estimator_topk<8,10000,sketch_type> cfe_topk8;
			chunk_freq_estimator_topk<16,10000,sketch_type> cfe_topk;
			chunk_freq_estimator_topk<32,10000,sketch_type> cfe_topk32;
			LOG(INFO) << "\t" << "Create dictionary with budget " << budget_mb << " MiB";
			LOG(INFO) << "\t" << "Block size = " << t_block_size; 
			LOG(INFO) << "\t" << "Num blocks = " << num_blocks_required; 

			sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
			// (1) create frequency estimates
			{
				LOG(INFO) << "\t" << "Sketch size = " << cfe_topk.sketch.size_in_bytes()/(1024*1024) << " MiB";
				LOG(INFO) << "\t" << "Sketch params = {d=" << cfe_topk.sketch.d << ",w=" << cfe_topk.sketch.w << "}";
				LOG(INFO) << "\t" << "Estimating block frequencies";
				auto est_start = hrclock::now();
				for(size_t i=0;i<text.size();i++) {
					auto sym = text[i];
					cfe_topk.update(sym);
					// cfe_topk8.update(sym);
					// cfe_topk32.update(sym);
				}
				auto est_stop = hrclock::now();
				auto est_error = cfe_topk.sketch.estimation_error();
				LOG(INFO) << "\t" << "Estimation time = " << duration_cast<milliseconds>(est_stop-est_start).count() / 1000.0f << " sec";
				LOG(INFO) << "\t" << "Sketch estimation error = " << est_error;
				LOG(INFO) << "\t" << "Sketch estimation confidence = " << cfe_topk.sketch.estimation_probability();

				auto topk_blocks = cfe_topk.topk();
				threshold = topk_blocks.back().first;
				LOG(INFO) << "\t" << "Threshold freq = " << threshold;
			}

			{
				/* uniform sampling */
	            std::vector<uint8_t> udict;
                auto num_samples = budget_bytes / t_block_size;
                LOG(INFO) << "\tUniform Dictionary samples = " << num_samples;
                auto n = text.size();
                size_t sample_step = n / num_samples;
                LOG(INFO) << "\tSample steps = " << sample_step;
                for (size_t i = 0; i < n; i += sample_step) {
                    for (size_t j = 0; j < t_block_size; j++) {
                        if (i + j >= n)
                            break;
                        udict.push_back(text[i + j]);
                    }
                }
                udict.push_back(0); // zero terminate for SA construction

                /* measure how many of the top-k things are in the uniform dict */
                LOG(INFO)  << "rank;found;chunksize\n";
                // auto topk8_blocks = cfe_topk8.topk();
                // for(size_t i=0;i<topk8_blocks.size();i++) {
                // 	auto ci = topk8_blocks[i].second;
                // 	auto start = ci.start;
                // 	if( std::search(udict.begin(),udict.end(),text.begin()+start,text.begin()+start+8) != udict.end() ) {
                // 		LOG(INFO)  << i+1 <<";"<<"1;8\n";
                // 	} else {
                // 		LOG(INFO)  << i+1 <<";"<<"0;8\n";
                // 	}
                // }
                auto topk16_blocks = cfe_topk.topk();
                for(size_t i=0;i<topk16_blocks.size();i++) {
                	auto ci = topk16_blocks[i].second;
                	auto start = ci.start;
                	if( std::search(udict.begin(),udict.end(),text.begin()+start,text.begin()+start+16) != udict.end() ) {
                		// LOG(INFO)  << i+1 <<";"<<"1;16\n";
                	} else {
                		LOG(INFO)  << i+1 <<";"<<"0;16";
                	}
                }
                // auto topk32_blocks = cfe_topk32.topk();
                // for(size_t i=0;i<topk32_blocks.size();i++) {
                // 	auto ci = topk32_blocks[i].second;
                // 	auto start = ci.start;
                // 	if( std::search(udict.begin(),udict.end(),text.begin()+start,text.begin()+start+32) != udict.end() ) {
                // 		LOG(INFO)  << i+1 <<";"<<"1;32\n";
                // 	} else {
                // 		LOG(INFO)  << i+1 <<";"<<"0;32\n";
                // 	}
                // }
 			}

			// (2) write dict 
			LOG(INFO) << "\t" << "Writing dictionary"; 
			auto dict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
			size_t dict_written = 0;
			LOG(INFO) << "\t" << "Performing second pass over text"; 

			rabin_karp_hasher<t_estimator_block_size> rk;
			std::unordered_set<uint64_t> dict_hashes_used;
			auto not_found = dict_hashes_used.end();
			bool in_chunk = false;
			size_t chunk_len = 0;
			size_t chunk_start = 0;
			size_t blocks_added = 0;
			size_t last_chunk_end = 0;
			size_t cur_block_len = 0;
			size_t max_block_len = 0;
			auto text_begin = text.begin();
			for(size_t i=0;i<t_estimator_block_size-1;i++) rk.update(text[i]); // init RKH
			for(size_t i=t_estimator_block_size-1;i<text.size();i++) {
				auto sym = text[i];
				auto hash = rk.update(sym);
				auto est = cfe_topk.estimate(hash);
				auto itr = dict_hashes_used.find(hash);
				if( est >= threshold || itr == not_found ) {
					if(in_chunk) chunk_len++;
					else {
						in_chunk = true;
						chunk_start = i - (t_estimator_block_size-1);
						chunk_len = t_block_size;
					}
					dict_hashes_used.insert(hash);
				} else {
					if(in_chunk) {
						auto begin = text.begin() + chunk_start;
						if(last_chunk_end > chunk_start ) {
							begin = text.begin() + last_chunk_end;
							chunk_len -= (last_chunk_end - chunk_start);
							cur_block_len += chunk_len;
						} else {
							cur_block_len = chunk_len;
							blocks_added++;
						}
						max_block_len = std::max(cur_block_len,max_block_len);
						auto end = begin + chunk_len;
						std::copy(begin,end,std::back_inserter(dict));
						dict_written += chunk_len;
						last_chunk_end = std::distance(text_begin,end);
						in_chunk = false;
					}
				}
				if(dict_written >= budget_bytes) {
					LOG(INFO) << "Found enough frequent content. Processed " 
							  << 100* (double)i / (double)text.size() 
							  << "% of the text";
					break;
				}
			}

			LOG(INFO) << "\t" << "Blocks found = " << blocks_added;
			LOG(INFO) << "\t" << "Max block len = " << max_block_len;
			LOG(INFO) << "\t" << "Average block len = " << dict_written / blocks_added;
			LOG(INFO) << "\t" << "Final dictionary size = " << dict_written/(1024*1024) << " MiB"; 

			dict.push_back(0); // zero terminate for SA construction
		} else {
			LOG(INFO) << "\t" << "Dictionary exists at '" << file_name << "'";
		}
		// compute a hash of the dict so we don't reconstruct things
		// later when we don't have to.
		col.compute_dict_hash();
	}
};
