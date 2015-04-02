#pragma once

#include "utils.hpp"
#include "easylogging++.h"

#include "sdsl/io.hpp"
#include "sdsl/construct.hpp"
#include "sdsl/int_vector_mapper.hpp"

const std::string KEY_PREFIX = "text.";
const std::string KEY_TEXT = "TEXT";
const std::string KEY_DICT = "DICT";
const std::string KEY_FACTORIZED_TEXT = "FACTORS";
const std::string KEY_BLOCKMAP = "BLOCKMAP";
const std::string KEY_BLOCKOFFSETS = "BLOCKOFFSETS";
const std::string KEY_BLOCKFACTORS = "BLOCKFACTORS";

const std::string PARAM_DICT_HASH = "DICT_HASH";

struct collection {
    bool rebuild = false;
    std::string path;
    std::map<std::string,std::string> param_map;
    std::map<std::string,std::string> file_map;
    collection(const std::string& p) : path(p+"/")
    {
        if (! utils::directory_exists(path)) {
            throw std::runtime_error("Collection path not found.");
        }
        // make sure all other dirs exist
        std::string index_directory = path+"/index/";
        utils::create_directory(index_directory);
        std::string tmp_directory = path+"/tmp/";
        utils::create_directory(tmp_directory);
        std::string results_directory = path+"/results/";
        utils::create_directory(results_directory);
        std::string patterns_directory = path+"/patterns/";
        utils::create_directory(patterns_directory);

        /* make sure the necessary files are present */
        auto file_name = path+"/"+KEY_PREFIX+KEY_TEXT;
        file_map[KEY_TEXT] = file_name;
        if (! utils::file_exists(path+"/"+KEY_PREFIX+KEY_TEXT)) {
            LOG(FATAL) << "Collection path does not contain text.";
            throw std::runtime_error("Collection path does not contain text.");
        } else {
            sdsl::read_only_mapper<8> text(file_map[KEY_TEXT]);
            LOG(INFO) << "Found input text with size " << text.size() /(1024.0*1024.0) << " MiB";
        }
    }

    std::string compute_dict_hash() {
        auto file_name = file_map[KEY_DICT];
        sdsl::read_only_mapper<8> dict(file_map[KEY_DICT]);
        auto crc32 = utils::crc((const uint8_t*)dict.data(),dict.size());
        param_map[PARAM_DICT_HASH] = std::to_string(crc32);
        return std::to_string(crc32);
    }

    void clear() {

    }
};


template<class t_sa_type>
t_sa_type
create_or_load_sa(collection& col,bool build_over_reverse_dict = false)
{
    using sa_type = t_sa_type;
    sa_type sa;
    auto dict_hash = col.param_map[PARAM_DICT_HASH];
    auto sa_file = col.path+"/index/sa-type-"+
                   sdsl::util::class_to_hash(sa)+"-"
                   "dhash="+dict_hash+
                   ".sdsl";
    if (! utils::file_exists(sa_file) || col.rebuild ) {
        LOG(INFO) << "\tConstruct and store search structure to " << sa_file;
        sdsl::cache_config cfg;
        cfg.delete_files = false;
        cfg.dir = col.path + "/tmp/";
        cfg.id = dict_hash;
        cfg.file_map[sdsl::conf::KEY_TEXT] = col.file_map[KEY_DICT];
        if(build_over_reverse_dict) {
            LOG(INFO) << "\tBuild over reverse dictionary";
            auto reverse_dict_file = col.file_map[KEY_DICT]+".rev";
            if(! utils::file_exists(reverse_dict_file) || col.rebuild) {
                LOG(INFO) << "\tStore reverse dictionary";
                sdsl::read_only_mapper<8> dict(col.file_map[KEY_DICT]);
                auto rdict = sdsl::write_out_buffer<8>::create(reverse_dict_file);
                rdict.resize(dict.size());
                // don't copy the 0 at the end
                std::reverse_copy(std::begin(dict), std::end(dict)-1, std::begin(rdict));
                rdict[dict.size()-1] = 0; // append zero for suffix sort
            }
            cfg.file_map[sdsl::conf::KEY_TEXT] = reverse_dict_file;
            LOG(INFO) << "\tConstruct index";
            sdsl::construct(sa,reverse_dict_file,cfg,1);
        } else {
            LOG(INFO) << "\tConstruct index";
            sdsl::construct(sa,col.file_map[KEY_DICT],cfg,1);
        }
        sdsl::store_to_file(sa,sa_file);
    } else {
        LOG(INFO) << "\tLoad search structure from " << sa_file;
        sdsl::load_from_file(sa,sa_file);
    }
    LOG(INFO) << "\tDone constructing search structure";
    return std::move(sa);
}
