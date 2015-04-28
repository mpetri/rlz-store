#include "utils.hpp"
#include "collection.hpp"

#include "indexes.hpp"

INITIALIZE_EASYLOGGINGPP


typedef struct cmdargs {
    std::string collection_dir;
    bool rebuild;
    uint32_t threads;
    bool verify;
} cmdargs_t;

void
print_usage(const char* program)
{
    fprintf(stdout,"%s -c <collection directory> \n",program);
    fprintf(stdout,"where\n");
    fprintf(stdout,"  -c <collection directory>  : the directory the collection is stored.\n");
    fprintf(stdout,"  -d <debug output>          : increase the amount of logs shown.\n");
    fprintf(stdout,"  -t <threads>               : number of threads to use during factorization.\n");
    fprintf(stdout,"  -f <force rebuild>         : force rebuild of structures.\n");
    fprintf(stdout,"  -v <verify index>          : verify the factorization can be used to recover the text.\n");
};

cmdargs_t
parse_args(int argc,const char* argv[])
{
    cmdargs_t args;
    int op;
    args.collection_dir = "";
    args.rebuild = false;
    args.verify = false;
    args.threads = 1;
    while ((op=getopt(argc,(char* const*)argv,"c:fdvt:")) != -1) {
        switch (op) {
            case 'c':
                args.collection_dir = optarg;
                break;
            case 'f':
                args.rebuild = true;
                break;
            case 't':
                args.threads = std::stoul(optarg);
                break;
            case 'd':
                el::Loggers::setLoggingLevel(el::Level::Trace);
                break;
            case 'v':
                args.verify=true;
                break;
        }
    }
    if (args.collection_dir=="") {
        std::cerr << "Missing command line parameters.\n";
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    return args;
}
int main(int argc,const char* argv[])
{
    utils::setup_logger(argc,argv,false);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    cmdargs_t args = parse_args(argc,argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    collection col(args.collection_dir);

    /* create rlz index */
    {
		const uint32_t dictionary_mem_budget_mb = 32;
		const uint32_t factorization_block_size = 2048;
		using dict_creation_strategy = dict_random_sample_budget<dictionary_mem_budget_mb,1024>;
		using rlz_type = rlz_store_static<
	                        dict_creation_strategy,
	                        default_dict_pruning_strategy,
	                        default_dict_index_type,
	                        factorization_block_size,
	                        default_factor_selection_strategy,
	                        default_factor_encoder,
	                        default_block_map>;
        auto rlz_store = rlz_type::builder{}
                            .set_rebuild(args.rebuild)
                            .set_threads(args.threads)
                            .build_or_load(col);

        std::vector<uint32_t> offset_cnts(rlz_store.dict.size());
        std::vector<uint32_t> offset_lens(rlz_store.dict.size());
        auto fitr = rlz_store.factors_begin();
        auto fend = rlz_store.factors_end();
        while(fitr != fend) {
        	const auto& f = *fitr;
        	if(f.len != 0) {
        		for(size_t i=0;i<f.len;i++) {
        			offset_cnts[f.offset+i]++;
        			offset_lens[f.offset+i] += f.len;
        		}
        	}
        	++fitr;
        }

        std::cout << "offset;freq;clen;avglen\n";
        for(size_t i=0;i<offset_cnts.size();i++) {
        	std::cout << i << ";" << offset_cnts[i] << ";" << offset_lens[i] << ";"
        			  << (double) offset_lens[i] / (double) offset_cnts[i] <<"\n";
        }
    }


    return EXIT_SUCCESS;
}
