#include "utils.hpp"
#include "collection.hpp"
#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP


int main(int argc, const char* argv[])
{
    setup_logger(argc, argv, false);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    cmdargs_t args = utils::parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* create rlz index */
    {
        auto rlz_store = rlz_type_standard::builder{}
                             .set_dict_size(args.dict_size_in_bytes)
                             .load(col);

        size_t prev = 0;
        for(size_t i=1;i<rlz_store.block_map.num_blocks();i++) {
        	auto offset = rlz_store.block_map.block_offset(i);
        	auto size_in_bits = offset - prev;
        	std::cout << i-1 << ";" << size_in_bits << "\n";
        }
        std::cout << rlz_store.block_map.num_blocks()-1 << ";"
        	<< rlz_store.text_size - rlz_store.block_map.block_offset(rlz_store.block_map.num_blocks()-1)
        	<< "\n";

    }

    return EXIT_SUCCESS;
}
