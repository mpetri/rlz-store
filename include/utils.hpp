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

bool is_root()
{
    return getuid() == 0;
}

void flush_cache() /* requires root */
{
    if (is_root()) {
        sync();
        {
            std::ofstream ofs("/proc/sys/vm/drop_caches");
            ofs << "3" << std::endl;
        }
        {
            std::ofstream ofs("/proc/sys/vm/drop_caches");
            ofs << "3" << std::endl;
        }
        {
            std::ofstream ofs("/proc/sys/vm/drop_caches");
            ofs << "3" << std::endl;
        }
    }
    else {
        throw std::runtime_error("need to be root to flush cache!");
    }
}

uint32_t
crc(const uint8_t* buf, size_t len)
{
    uint32_t crc_val = crc32(0L, Z_NULL, 0);
    crc_val = crc32(crc_val, buf, (uint32_t)len);
    return crc_val;
}

bool directory_exists(std::string dir)
{
    struct stat sb;
    const char* pathname = dir.c_str();
    if (stat(pathname, &sb) == 0 && (S_IFDIR & sb.st_mode)) {
        return true;
    }
    return false;
}

bool file_exists(std::string file_name)
{
    std::ifstream in(file_name);
    if (in) {
        in.close();
        return true;
    }
    return false;
}

bool symlink_exists(std::string file)
{
    struct stat sb;
    const char* filename = file.c_str();
    if (stat(filename, &sb) == 0 && (S_IFLNK & sb.st_mode)) {
        return true;
    }
    return false;
}

void create_directory(std::string dir)
{
    if (!directory_exists(dir)) {
        if (mkdir(dir.c_str(), 0777) == -1) {
            std::perror("could not create directory");
            exit(EXIT_FAILURE);
        }
    }
}

void rename_file(std::string from, std::string to)
{
    int rc = std::rename(from.c_str(), to.c_str());
    if (rc) {
        perror("error renaming file");
        exit(EXIT_FAILURE);
    }
}

void remove_file(std::string file)
{
    std::remove(file.c_str());
}

void remove_all_files_in_dir(std::string dir)
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
    size_t pruned_dict_size_in_bytes;
    bool rebuild;
    uint32_t threads;
    bool verify;
} cmdargs_t;

void print_usage(const char* program)
{
    fprintf(stdout, "%s <args>\n", program);
    fprintf(stdout, "where\n");
    fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
    fprintf(stdout, "  -s <dict size in MB>       : size of the initial dictionary in MB.\n");
    fprintf(stdout, "  -p <dict size in MB>       : size of the dictionary after pruning in MB.\n");
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
    args.pruned_dict_size_in_bytes = 0;
    while ((op = getopt(argc, (char* const*)argv, "c:fdvt:s:p:")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
            break;
        case 's':
            args.dict_size_in_bytes = std::stoul(optarg) * (1024 * 1024);
            break;
        case 'f':
            args.rebuild = true;
            break;
        case 'p':
            args.pruned_dict_size_in_bytes = std::stoul(optarg) * (1024 * 1024);
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
    if (args.collection_dir == "") {
        std::cerr << "Missing command line parameters.\n";
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    if (args.pruned_dict_size_in_bytes == 0) {
        args.pruned_dict_size_in_bytes = args.dict_size_in_bytes;
    }

    return args;
}

using namespace std::chrono;
using watch = std::chrono::high_resolution_clock;

template <class t_dur = std::chrono::milliseconds>
struct rlz_timer {
    watch::time_point start;
    std::string name;
    bool output;
    rlz_timer(const std::string& _n, bool o = true)
        : name(_n)
        , output(o)
    {
        if (output)
            LOG(INFO) << "START(" << name << ")";
        start = watch::now();
    }
    ~rlz_timer()
    {
        auto stop = watch::now();
        auto time_spent = stop - start;
        if (output)
            LOG(INFO) << "STOP(" << name << ") - " << duration_cast<t_dur>(time_spent).count();
    }
    watch::duration
    elapsed() const
    {
        return watch::now() - start;
    }
};

    
constexpr uint32_t max_pos_len = 512;

struct qgram_postings {
    uint32_t pos[max_pos_len];
    uint32_t last = 0;
    uint32_t items = 0;
    qgram_postings(size_t p) {
        pos[0] = p;
        last = 1;
        items = 1;
    }
};
    

} // end of util namespace
