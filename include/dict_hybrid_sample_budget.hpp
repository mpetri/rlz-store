#pragma once

#include "utils.hpp"
#include "collection.hpp"

template <uint32_t t_block_size_bytes>
class dict_hybrid_sample_budget {
public:
    static std::string type()
    {
        return "dict_hybrid_sample_budget-"
               + std::to_string(t_block_size_bytes);
    }

    static std::string file_name(collection& col,uint64_t size_in_bytes)
    {
        auto size_in_mb = size_in_bytes / (1024*1024);
        return col.path + "/index/" + type()+"-"+std::to_string(size_in_mb) + ".sdsl";
    }

public:
    static void create(collection& col, bool rebuild,size_t size_in_bytes)
    {
        const uint32_t block_size = t_block_size_bytes;
        uint64_t budget_bytes = size_in_bytes;
        uint64_t budget_mb = budget_bytes / (1024*1024);
        // check if we store it already and load it
        auto fname = file_name(col,size_in_bytes);
        col.file_map[KEY_DICT] = fname;
        if (!utils::file_exists(fname) || rebuild) { // construct
            LOG(INFO) << "\tCreate dictionary with budget " << budget_mb << " MiB";

            // memory map the text and iterate over it
            std::vector<uint8_t> dict;
            {
		bool b_keep_hybrid_dictionary = true;
		if ( b_keep_hybrid_dictionary )
            		LOG(INFO) << "\tCreating hybrid dictionary " ;
		else
            		LOG(INFO) << "\tCreating regular dictionary " ;


                sdsl::read_only_mapper<8> text(col.file_map[KEY_TEXT]);
		size_t number_of_samples =  budget_bytes / block_size;
		//earlier i wud dip in at equally spaced apart number_of_samples points and sample 1 block size each.. now 
		// i have a pattern of samplign over data between current sample point to next sample point
		size_t pattern_size =  text.size()/  ( number_of_samples);  

		for(size_t location =0; location < text.size() ; location += (2*pattern_size))
		{
			if ( b_keep_hybrid_dictionary )
			{
				std::copy(text.begin()+location, 
					  text.begin()+location+ block_size, std::back_inserter(dict));
	
				std::copy(text.begin()+location + pattern_size/2, 
					  text.begin()+location + pattern_size/2 + block_size/4, std::back_inserter(dict));

				std::copy(text.begin()+location + pattern_size, 
					  text.begin()+location + pattern_size  + block_size/2,	std::back_inserter(dict));

            			std::copy(text.begin()+location + 3*pattern_size/2, 
					  text.begin()+location + 3*pattern_size/2 + block_size/4, std::back_inserter(dict));

            			
			}else
			{ //revert to default dictionary
				
				std::copy(text.begin()+location, 
					  text.begin()+location+ block_size, std::back_inserter(dict));

            			std::copy(text.begin()+location + pattern_size, 
					  text.begin()+location + pattern_size + block_size, std::back_inserter(dict));

			}
		}


                dict.push_back(0); // zero terminate for SA construction
            }

            /* store to disk */
            auto wdict = sdsl::write_out_buffer<8>::create(col.file_map[KEY_DICT]);
            std::copy(dict.begin(),dict.end(),std::back_inserter(wdict));
	     LOG(INFO) << "\t" << "Wrote dictionary of size "<< dict.size()<< "bytes";
        } else {
            LOG(INFO) << "\t"
                      << "Dictionary exists at '" << fname << "'";
        }
        // compute a hash of the dict so we don't reconstruct things
        // later when we don't have to.
        col.compute_dict_hash();
    }
};
