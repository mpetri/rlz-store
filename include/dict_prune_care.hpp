#pragma once

#include <ratio>

#include "utils.hpp"
#include "collection.hpp"

enum EST_TYPE : int
{
	FF,
	FFT 
};

template<
uint64_t t_freq_threshold,
uint64_t t_length_threshold,
EST_TYPE t_method = FF
>
struct dict_prune_care {
	struct segment_info {
		uint64_t offset;
		uint64_t length;
		uint64_t total_byte_usage;
		uint64_t num_factors_req;
		segment_info() = default;
		segment_info(uint64_t o,uint64_t l,uint64_t t,uint64_t n)
			: offset(o), length(l), total_byte_usage(t), num_factors_req(n) {}
	};

    static std::string type()
    {
        return "dict_pruned_care-" + std::to_string(t_freq_threshold) +"-"+ std::to_string(t_length_threshold) +"-"+ std::to_string(t_method);
    }

    static std::string file_name(collection& col, uint64_t size_in_bytes,std::string dhash)
    {
        auto size_in_mb = size_in_bytes / (1024 * 1024);
        return col.path + "/index/" + type() + "-" + std::to_string(size_in_mb) + "-dhash=" + dhash +  ".sdsl";
    }

    template <class t_dict_idx>
    static void compute_nfac(t_dict_idx& idx,const sdsl::int_vector_mapper<8, std::ios_base::in>& dict,segment_info& segment) {
    	auto seg_start = dict.begin()+segment.offset;
    	auto seg_end = seg_start + segment.length;
        auto factor_itr = idx.factorize_restricted(seg_start, seg_end);
        uint64_t factors_produced = 0;
        while (!factor_itr.finished()) {
            factors_produced++;
            ++factor_itr;
        }
        segment.num_factors_req = factors_produced;
    }

    template <class t_dict_idx,class t_factorization_strategy>
    static void prune(collection& col, bool rebuild,uint64_t target_dict_size_bytes,uint64_t num_threads)
    {
    	auto start_dict_size_bytes = 0ULL;
    	{
    		const sdsl::int_vector_mapper<8, std::ios_base::in> dict(col.file_map[KEY_DICT]);
    		start_dict_size_bytes = dict.size();
    	}
    	if(start_dict_size_bytes == target_dict_size_bytes) {
        	LOG(INFO) << "\t"
                  	<< "No dictionary pruning necessary.";
            return;
    	}
    	auto dict_hash = col.param_map[PARAM_DICT_HASH];
    	auto new_dict_file = file_name(col,target_dict_size_bytes,dict_hash);
    	if (!rebuild && utils::file_exists(new_dict_file)) {
            LOG(INFO) << "\t"
                      << "Pruned dictionary exists at '" << new_dict_file << "'";
    		col.file_map[KEY_DICT] = new_dict_file;
    		col.compute_dict_hash();
    		return;
    	}

    	/* (1) create or load statistics */
        LOG(INFO) << "\t" << "Create or load statistics.";
    	auto dict_file = col.file_map[KEY_DICT];
    	auto dict_stats_file = dict_file + "-" + KEY_DICT_STATISTICS + "-" + t_factorization_strategy::type() + ".sdsl";
    	factorization_statistics fstats;
    	if (rebuild || !utils::file_exists(dict_stats_file)) {
	        fstats = t_factorization_strategy::template parallel_factorize<factor_tracker>(col,rebuild,num_threads);
	        sdsl::store_to_file(fstats,dict_stats_file);
	    } else {
	    	sdsl::load_from_file(fstats,dict_stats_file);
	    }

    	/* (2) find segments */
    	LOG(INFO) << "\t" << "Find potential segments to remove.";

        size_t total_len = 0;
    	auto bytes_to_remove = start_dict_size_bytes - target_dict_size_bytes;
        auto freq_threshold = t_freq_threshold;
    	std::vector<segment_info> segments;
        while( total_len < bytes_to_remove ) {
            segments.clear();
            total_len = 0;
    	    size_t run_len = 0;
    	    size_t total_byte_usage = 0;
    	    for(size_t i=0;i<fstats.dict_usage.size();i++) {
    		    if(fstats.dict_usage[i] <= freq_threshold) {
    			    run_len++;
    			    total_byte_usage += fstats.dict_usage[i];
    		    } else {
    			    if(run_len >= t_length_threshold) {
    				    auto seg_start = i - run_len;
    				    segments.emplace_back(seg_start,run_len,total_byte_usage,0);
    				    total_len += run_len;
    			    }
    			    run_len = 0;
    			    total_byte_usage = 0;
    		    }
    	    }
            LOG(INFO) << "Freq threshold = " << freq_threshold << " Found bytes = " << total_len << " Req = " << bytes_to_remove;
            freq_threshold *= 2;
        }
        LOG(INFO) << "\t" << "Freq threshold = " << freq_threshold << " Length threshold = " << t_length_threshold;
    	LOG(INFO) << "\t" << "Found " << segments.size() << " segments of total length " << total_len << " (" << total_len/(1024*1024) << " MiB)";

    	/* (3) compute the metric for those segments */
        {
        	LOG(INFO) << "Create/Load dictionary index";
        	t_dict_idx idx(col, rebuild);
        	const sdsl::int_vector_mapper<8, std::ios_base::in> dict(col.file_map[KEY_DICT]);
        	for(size_t i=0;i<segments.size();i++) {
        		compute_nfac(idx,dict,segments[i]);
        	}
        }
        /* (3) sort by method */
        LOG(INFO) << "Sort segments by weight";
        if(t_method == FF) {
        	/* FF */
        	std::sort(segments.begin(),segments.end(),[](const segment_info& a,const segment_info& b) {
        		double score_a = (double)a.num_factors_req * ((double)a.total_byte_usage/(double)a.length);
        		double score_b = (double)b.num_factors_req * ((double)b.total_byte_usage/(double)b.length);
        		return score_a < score_b;
        	});
        } else {
        	/* FFT */
        	std::sort(segments.begin(),segments.end(),[](const segment_info& a,const segment_info& b) {
        		double score_a = (double)a.num_factors_req * ((double)a.total_byte_usage/(double)a.length);
        		score_a = score_a / (double)a.length;
        		double score_b = (double)b.num_factors_req * ((double)b.total_byte_usage/(double)b.length);
        		score_b = score_b / (double)b.length;
        		return score_a < score_b;
        	});
        }

        size_t segments_to_remove = 0;
        size_t segment_cum_len = 0;
        for(size_t i=0;i<segments.size();i++) {
        	segments_to_remove++;
        	segment_cum_len += segments[i].length;
        	if(segment_cum_len >= bytes_to_remove) {
        		// update the length so it fits into the size we want 
        		auto new_len = segment_cum_len - bytes_to_remove;
        		segments[i].length -= (new_len + 1);
        		break;
        	}
        }
        segments.resize(segments_to_remove);
        LOG(INFO) << "Selected " << segments_to_remove << " for removal";

        LOG(INFO) << "Creating pruned dictionary";
        LOG(INFO) << "Sorting segments into offset order";
    	std::sort(segments.begin(),segments.end(),[](const segment_info& a,const segment_info& b) {
    		return a.offset < b.offset;
    	});
    	{
    		const sdsl::int_vector_mapper<8, std::ios_base::in> dict(col.file_map[KEY_DICT]);
    		auto wdict = sdsl::write_out_buffer<8>::create(new_dict_file);
    		size_t cur_segment = 0;
    		for(size_t i=0;i<dict.size()-2;) {
    			if(segments[cur_segment].offset == i) {
    				/* skip the segment */
    				i += segments[cur_segment].length;
    				cur_segment++;
    			} else {
    				wdict.push_back(dict[i]);
    				i++;
    			}
    		}
    		wdict.push_back(0);
    		LOG(INFO) << "\t" << "Pruned dictionary size = " << wdict.size() / (1024*1024) << " MiB";
    	}

		col.file_map[KEY_DICT] = new_dict_file;
		col.compute_dict_hash();
    }
};
