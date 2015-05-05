#pragma once

#include <string>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdio>
#include <chrono>

#include <zlib.h>
#include <dirent.h>

#include "easylogging++.h"

using hrclock = std::chrono::high_resolution_clock;

namespace utils {

uint32_t
crc(const uint8_t* buf, size_t len)
{
    uint32_t crc_val = crc32(0L, Z_NULL, 0);
    crc_val = crc32(crc_val, buf, (uint32_t)len);
    return crc_val;
}

bool
directory_exists(std::string dir)
{
    struct stat sb;
    const char* pathname = dir.c_str();
    if (stat(pathname, &sb) == 0 && (S_IFDIR & sb.st_mode)) {
        return true;
    }
    return false;
}

bool
file_exists(std::string file_name)
{
    std::ifstream in(file_name);
    if (in) {
        in.close();
        return true;
    }
    return false;
}

bool
symlink_exists(std::string file)
{
    struct stat sb;
    const char* filename = file.c_str();
    if (stat(filename, &sb) == 0 && (S_IFLNK & sb.st_mode)) {
        return true;
    }
    return false;
}

void
create_directory(std::string dir)
{
    if (!directory_exists(dir)) {
        if (mkdir(dir.c_str(), 0777) == -1) {
            std::perror("could not create directory");
            exit(EXIT_FAILURE);
        }
    }
}

void
rename_file(std::string from, std::string to)
{
    int rc = std::rename(from.c_str(), to.c_str());
    if (rc) {
        perror("error renaming file");
        exit(EXIT_FAILURE);
    }
}

void
remove_file(std::string file)
{
    std::remove(file.c_str());
}

void
remove_all_files_in_dir(std::string dir)
{
    struct dirent* next_file;
    auto folder = opendir(dir.c_str());
    while ((next_file = readdir(folder)) != nullptr) {
        std::string file_name = (const char*)next_file->d_name;
        if (file_name != "." && file_name != "..") {
            auto file_path = dir + "/" + file_name;
            remove_file(file_path);
        }
    }
}


template <class t_itr>
std::string safe_print(t_itr itr, t_itr end)
{
    std::string str;
    while (itr != end) {
        uint8_t sym = *itr;
        if (std::isspace(sym))
            str += " ";
        else if (std::isprint(sym))
            str += sym;
        else
            str += "?";
        ++itr;
    }
    return str;
}

typedef struct cmdargs {
    std::string collection_dir;
    size_t dict_size_in_bytes;
    bool rebuild;
    uint32_t threads;
    bool verify;
} cmdargs_t;

void
print_usage(const char* program)
{
    fprintf(stdout, "%s <args>\n", program);
    fprintf(stdout, "where\n");
    fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
    fprintf(stdout, "  -s <dict size in MB>       : size of the dictionary in MB.\n");
    fprintf(stdout, "  -d <debug output>          : increase the amount of logs shown.\n");
    fprintf(stdout, "  -t <threads>               : number of threads to use during factorization.\n");
    fprintf(stdout, "  -f <force rebuild>         : force rebuild of structures.\n");
    fprintf(stdout, "  -v <verify index>          : verify the factorization can be used to recover the text.\n");
};

cmdargs_t
parse_args(int argc, const char* argv[])
{
    cmdargs_t args;
    int op;
    args.collection_dir = "";
    args.rebuild = false;
    args.verify = false;
    args.threads = 1;
    args.dict_size_in_bytes = 0;
    while ((op = getopt(argc, (char* const*)argv, "c:fdvt:s:")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
            break;
        case 's':
            args.dict_size_in_bytes = std::stoul(optarg)*(1024*1024);
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
            args.verify = true;
            break;
        }
    }
    if (args.collection_dir == "" || args.dict_size_in_bytes == 0) {
        std::cerr << "Missing command line parameters.\n";
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    return args;
}


} // end of util namespace
