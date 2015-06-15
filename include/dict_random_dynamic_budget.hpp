#pragma once

#include "utils.hpp"
#include "collection.hpp"

template <uint32_t t_block_size_bytes = 1024>
class dict_random_dynamic_budget {
public:
    static std::string type()
    {
        return "dict_random_dynamic_budget-"
               + std::to_string(t_block_size_bytes);
    }

    static std::string file_name(collection& col,uint64_t size_in_bytes)
    {

	    time_t timer;
	    struct tm y2k = {0};
	    double seconds;
	    y2k.tm_hour = 0;   y2k.tm_min = 0; y2k.tm_sec = 0;
	    y2k.tm_year = 100; y2k.tm_mon = 0; y2k.tm_mday = 1;
	    time(&timer);  /* get current time; same as: timer = time(NULL)  */
	    seconds = difftime(timer,mktime(&y2k));

	    std::string timestamp = std::to_string(seconds);
	    auto size_in_mb = size_in_bytes / (1024*1024);
	    return col.path + "/index/" + type()+"-"+std::to_string(size_in_mb) +".mb."+ timestamp +".sdsl";
    }

public:
	
    static void create(collection& col, bool rebuild,size_t size_in_bytes)
    {
        const uint32_t block_size = t_block_size_bytes;
        uint64_t budget_bytes = size_in_bytes;
        uint64_t budget_mb = budget_bytes / (1024*1024);


        // create a random dictionary afresh, for every run 
        auto fname = file_name(col,size_in_bytes);
        col.file_map[KEY_DICT] = fname;
	{ // construct
            LOG(INFO) << "\tCreate dictionary with budget " << budget_mb << " MiB";
            // memory map the text and iterate over it

            std::vector<uint8_t> dict;
            {
                sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
                auto num_samples = budget_bytes / block_size;
                auto n = text.size();
                size_t sample_step = n / num_samples;
                LOG(INFO) << "\tDictionary samples = " << num_samples;
	
		std::vector<uint32_t> sample_ids(n / block_size) ; 
		for (long i=0; i < (n/block_size); i++) sample_ids[i] = i;
		std::srand(std::time(0));
		std::random_shuffle(sample_ids.begin(), sample_ids.end());
		sample_ids.resize(num_samples);
		std::sort(sample_ids.begin(), sample_ids.end());

        LOG(INFO) << "\tSample steps = " << sample_step;
		LOG(INFO) << "\t\t few sample blocks="<< sample_ids[0]<<","<<sample_ids[1]<<sample_ids[2]<<","<<sample_ids[3];
		for( auto sample_iterator= sample_ids.begin(); sample_iterator != sample_ids.end(); sample_iterator++)
		{
		    size_t i = (*sample_iterator)*block_size;
            for (size_t j = 0; j < block_size; j++) 
		    {
                        //if (i + j >= n){   break;}
			         assert( i+j < n );
                        dict.push_back(text[i + j]);
                    }
                }
                dict.push_back(0); // zero terminate for SA construction
            }

            /* store to disk */
            LOG(INFO) << "\t" << "Writing dictionary.";
            auto wdict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
            std::copy(dict.begin(),dict.end(),std::back_inserter(wdict));
        }
	
	    // compute a hash of the dict so we don't reconstruct things
        // later when we don't have to.
        col.compute_dict_hash();
    }
};
