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
			chunk_freq_estimator_topk<16,500000,sketch_type> cfe_topk;
			sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);

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
					for(size_t i=0;i<text.size();i++) {
						auto sym = text[i];
						cfe_topk.update(sym);
					}
					auto est_stop = hrclock::now();
					auto est_error = cfe_topk.sketch.estimation_error();
					LOG(INFO) << "\t" << "Estimation time = " << duration_cast<milliseconds>(est_stop-est_start).count() / 1000.0f << " sec";
					LOG(INFO) << "\t" << "Sketch estimation error = " << est_error;
					LOG(INFO) << "\t" << "Sketch estimation confidence = " << cfe_topk.sketch.estimation_probability();

					auto topk_blocks = cfe_topk.topk();
					threshold = topk_blocks.back().first;
					LOG(INFO) << "\t" << "Threshold freq = " << threshold;
					sdsl::store_to_file(cfe_topk,sketch_name);
				}
			} else {
				LOG(INFO) << "\t" << "Load sketch from file " << sketch_name;
				sdsl::load_from_file(cfe_topk,sketch_name);
			}

			// (2) write dict 
			LOG(INFO) << "\t" << "Writing dictionary"; 
			auto dict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
			size_t dict_written = 0;

			std::unordered_set<uint64_t> dict_blocks;
			size_t blocks_found = 0; size_t len = 0;
			rabin_karp_hasher<t_estimator_block_size> rk;
			for(size_t i=0;i<t_estimator_block_size;i++) rk.update(text[i]); // init RKH
			for(size_t i=t_estimator_block_size;i<text.size();i++) {
				auto sym = text[i];
				auto hash = rk.update(sym);
				auto est_freq = cfe_topk.estimate(hash);
				if(est_freq > threshold) {
					if( dict_blocks.find(hash) == dict_blocks.end() ) {
						len++;
						dict_blocks.insert(hash);
					} else {
						len = 0;
					}
				} else {
					if(len) {
						auto begin = text.begin() + i - len;
						auto end = begin + t_block_size + len;
						dict_written += t_block_size+len;
						len = 0;
						std::copy(begin,end,std::back_inserter(dict));
						blocks_found++;
					}
				}
				if(dict_written > budget_bytes) {
					LOG(INFO) << "\t" << "Text scanned = " << 100*(float)i / (float)text.size();
					break;
				}
			}
			LOG(INFO) << "\t" << "Blocks found = " << blocks_found;
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
