#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "logging.hpp"

#include <unordered_set>
#include <string>

using namespace std::chrono;

template <uint32_t t_block_size = 1024,
    uint32_t t_estimator_block_size = 16,
    uint32_t t_down_size = 256,
    class t_norm = std::ratio<1, 1>,
    ACCESS_TYPE t_method = SEQ>
class dict_assemble_disjoint_gsc {
public:
    static std::string type()
    {
        return "dict_assemble_disjoint_gsc-" + std::to_string(t_method) + "-" + std::to_string(t_block_size) + "-" + std::to_string(t_estimator_block_size);
    }
    static uint32_t adjusted_down_size(collection& col, uint64_t size_in_bytes)
    {
        sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
        //auto ratio = (text.size()/size_in_bytes)/2;
        //return (ratio >= t_down_size? t_down_size : ratio);
        return 256; //for 10gb
    }

    static std::string container_type()
    {
        return std::to_string(t_estimator_block_size);
    }
    static std::string dict_file_name(collection& col, uint64_t size_in_bytes)
    {
        auto size_in_mb = size_in_bytes / (1024 * 1024);
        return col.path + "/index/" + type() + "-" + std::to_string(size_in_mb) + "-" + std::to_string(t_norm::num) + "-" + std::to_string(t_norm::den) + +"-" + std::to_string(adjusted_down_size(col, size_in_bytes)) + ".sdsl";
    }
    static std::string container_file_name(collection& col, uint64_t size_in_bytes)
    {
        auto size_in_mb = size_in_bytes / (1024 * 1024);
        return col.path + "/index/" + container_type() + ".sdsl";
    }

public:
    static void create(collection& col, bool rebuild, size_t size_in_bytes)
    {
        uint32_t budget_bytes = size_in_bytes;
        uint32_t budget_mb = size_in_bytes / (1024 * 1024);

        // check if we store it already and load it
        auto fname = dict_file_name(col, size_in_bytes);
        col.file_map[KEY_DICT] = fname;
        auto down_size = adjusted_down_size(col, size_in_bytes);

        if (!utils::file_exists(fname) || rebuild) { // construct
            auto start_total = hrclock::now();
            LOG(INFO) << "\t"
                      << "Create dictionary with budget " << budget_mb << " MiB";
            LOG(INFO) << "\t"
                      << "Block size = " << t_block_size;

            sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
            auto n = text.size();

            //1st pass: reservoir sampling for frequent k-mers
            auto rs_name = container_file_name(col, size_in_bytes) + "-DisjointRSample-" + std::to_string(down_size);
            //	fixed_hasher<t_estimator_block_size> rk;

            uint64_t rs_size = (text.size() - t_estimator_block_size + 1) / down_size;
            std::vector<uint64_t> rs;
            if (!utils::file_exists(rs_name) || rebuild) {
                auto start = hrclock::now();
                LOG(INFO) << "\t"
                          << "Building Reservoir sample with downsize: " << down_size;
                //make a reservoir sampler and keep it in RAM
                uint64_t count = 0;
                uint64_t skip = 0;
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_real_distribution<double> dis(0, 1);

                //double r = dis(gen);
                double w = std::exp(std::log(dis(gen)) / rs_size);
                double s = std::floor(std::log(dis(gen)) / std::log(1 - w));
                std::hash<std::string> hash_fn;
                for (size_t i = 0; i < text.size() - t_estimator_block_size + 1; i = i + t_estimator_block_size) {
                    //					auto sym = text[i];
                    //					auto hash = rk.update(sym);
                    auto start = text.begin() + i;
                    std::string seg(start, start + t_estimator_block_size);
                    size_t hash = hash_fn(seg);

                    if (count < rs_size)
                        rs.push_back(hash);
                    else {
                        skip++;
                        // std::cout.precision(20);
                        // std::cout << "\t" << "w=" << w << " s=" << s;
                        //	std::uniform_int_distribution<uint64_t> dis(0, count);
                        //	uint64_t pos = dis(gen);
                        //	if(pos < rs_size)
                        //		rs[pos] = hash;
                        if (s + 1 <= text.size() - t_estimator_block_size) {
                            if (skip == s + 1) {
                                // LOG(INFO) << "\t" << "here: " << count;
                                rs[1 + std::floor(rs_size * dis(gen))] = hash;
                                w *= std::exp(std::log(dis(gen)) / rs_size);
                                s = std::floor(std::log(dis(gen)) / std::log(1 - w));
                                skip = 0;
                            }
                        }
                        else
                            break;
                    }
                    count++;
                }
                auto stop = hrclock::now();
                LOG(INFO) << "\t"
                          << "Reservoir sampling time = " << duration_cast<milliseconds>(stop - start).count() / 1000.0f << " sec";
                LOG(INFO) << "\t"
                          << "Store reservoir sample to file " << rs_name;
                // sdsl::store_to_file(rs,rs_name);

                auto rs_file = sdsl::write_out_buffer<64>::create(rs_name);
                {
                    auto itr = rs.begin();
                    while (itr != rs.end()) {
                        std::copy(itr, itr + 1, std::back_inserter(rs_file));
                        itr++;
                    }
                }
            }
            else {
                LOG(INFO) << "\t"
                          << "Load reservoir sample from file " << rs_name;
                // sdsl::load_from_file(rs,sketch_name);
                sdsl::read_only_mapper<64> rs_file(rs_name);
                auto itr = rs_file.begin();
                while (itr != rs_file.end()) {
                    rs.push_back(*itr);
                    itr++;
                }
            }

            LOG(INFO) << "\t"
                      << "Reservoir sample blocks = " << rs.size();
            LOG(INFO) << "\t"
                      << "Reservoir sample size = " << rs.size() * 8 / (1024 * 1024) << " MiB";
            //build exact counts of sampled elements
            LOG(INFO) << "\t"
                      << "Calculating exact frequencies of small rolling blocks...";
            std::unordered_map<uint64_t, uint32_t> block_counts;
            block_counts.max_load_factor(0.1);
            for (uint64_t s : rs) {
                block_counts[s]++;
            }
            rs.clear();

            // std::move(rs.begin(), rs.end(), std::inserter(useful_blocks, useful_blocks.end()));
            LOG(INFO) << "\t"
                      << "Useful kept small blocks no. = " << block_counts.size();
/*
			//2nd pass: assembe k-mers to contigs and then to dictionary of undermined size
			LOG(INFO) << "\t" << "Assembling and initial dic construction..."; 
			auto start = hrclock::now();
			auto dict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
			uint64_t beginPos = 0;
			uint64_t numSeg = 0;
			bool found = false;
			std::unordered_set<uint64_t> uniqueSegs;
			uniqueSegs.max_load_factor(0.1);
			std::hash<std::string> hash_fn;
			for(size_t i = 0; i < text.size()-t_estimator_block_size+1;i=i+t_estimator_block_size) {				
					auto start = text.begin() + i;
                                        std::string seg(start,start+t_estimator_block_size);
                                        size_t hash = hash_fn(seg);
					if(block_counts.find(hash) != block_counts.end()) //assemble
						found = true;
					else {//write and skip
						if(found == true) {//write while removing duplicates
						auto beg = text.begin() + beginPos;
					    	auto end = text.begin() + i;
					    	std::string seg(beg,end);
					    	size_t seg_hash = hash_fn(seg);

					    	if((i - beginPos >= 256 && i - beginPos <= 2048) && uniqueSegs.find(seg_hash) == uniqueSegs.end()) {
					    		uniqueSegs.emplace(seg_hash);
								std::copy(beg,end,std::back_inserter(dict));
								numSeg++;
								LOG(INFO) << "\t" << "Unique Segments: " << numSeg << "  Length: " << i - beginPos; 
					    	}
							LOG(INFO) << "\t" << "Completed: " << (double)i*100/text.size() << "\%"; 
							beginPos = i+t_estimator_block_size;
							found = false;
						} 
						else if (found == true &&  (i - beginPos < 256 || i - beginPos > 2048)) {
							beginPos = i+t_estimator_block_size;
							found = false;
						} 
						else {
							beginPos+=t_estimator_block_size;
						}
					}
			} 
			block_counts.clear();	
			uniqueSegs.clear();
			LOG(INFO) << "\t" << "Final number of frequent segments = " << numSeg; 
			LOG(INFO) << "\t" << "Final dictionary size = " << dict.size()/(1024*1024) << " MiB"; 
			dict.push_back(0); // zero terminate for SA construction
			auto stop = hrclock::now();																																																																																																																																																																																																																																																																																																																																																																																																																					
		    LOG(INFO) << "\t" << "Assemble and construct dic runtime = " << duration_cast<milliseconds>(stop-start).count() / 1000.0f << " sec";
	*/	}
else {
    LOG(INFO) << "\t"
              << "Dictionary exists at '" << fname << "'";
}
// compute a hash of the dict so we don't reconstruct things
// later when we don't have to.
//		col.compute_dict_hash();

//TODO: 3rd pass: using global sc or another algorithm to process subsampled and glued text
    }
};
