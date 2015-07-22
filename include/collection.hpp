#pragma once

#include "utils.hpp"
#include "logging.hpp"

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
const std::string KEY_FCODER = "FCODER";

const std::string PARAM_DICT_HASH = "DICT_HASH";

const std::string KEY_LZ = "LZ";
const std::string KEY_LZ_OFFSETS = "LZ_OFFSETS";
const std::string KEY_LZ_LEN = "LZ_LEN";

struct collection {
    std::string path;
    std::map<std::string, std::string> param_map;
    std::map<std::string, std::string> file_map;
    collection(const std::string& p)
        : path(p + "/")
    {
        if (!utils::directory_exists(path)) {
            throw std::runtime_error("Collection path not found.");
        }
        // make sure all other dirs exist
        std::string index_directory = path + "/index/";
        utils::create_directory(index_directory);
        std::string tmp_directory = path + "/tmp/";
        utils::create_directory(tmp_directory);
        std::string results_directory = path + "/results/";
        utils::create_directory(results_directory);
        std::string patterns_directory = path + "/patterns/";
        utils::create_directory(patterns_directory);

        /* make sure the necessary files are present */
        auto file_name = path + "/" + KEY_PREFIX + KEY_TEXT;
        file_map[KEY_TEXT] = file_name;
        if (!utils::file_exists(path + "/" + KEY_PREFIX + KEY_TEXT)) {
            LOG(FATAL) << "Collection path does not contain text.";
            throw std::runtime_error("Collection path does not contain text.");
        } else {
            sdsl::read_only_mapper<8> text(file_map[KEY_TEXT]);
            LOG(INFO) << "Found input text with size " << text.size() / (1024.0 * 1024.0) << " MiB";
        }
    }

    std::string compute_dict_hash()
    {
        auto file_name = file_map[KEY_DICT];
        sdsl::read_only_mapper<8> dict(file_map[KEY_DICT]);
        auto crc32 = utils::crc((const uint8_t*)dict.data(), dict.size());
        param_map[PARAM_DICT_HASH] = std::to_string(crc32);
        return std::to_string(crc32);
    }

    std::string temp_file_name(const std::string& key, size_t offset)
    {
        auto file_name = path + "/tmp/" + key + "-" + std::to_string(offset) + "-" + std::to_string(getpid()) + ".sdsl";
        return file_name;
    }

    void clear()
    {
        auto tmp_folder = path + "/tmp/";
        utils::remove_all_files_in_dir(tmp_folder);
    }
};
