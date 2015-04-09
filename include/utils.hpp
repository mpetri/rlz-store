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

namespace utils
{

uint32_t 
crc(const uint8_t* buf,size_t len) {
    uint32_t crc_val = crc32(0L,Z_NULL,0);
    crc_val = crc32(crc_val,buf,(uint32_t)len);
    return crc_val;
}

bool
directory_exists(std::string dir)
{
    struct stat sb;
    const char* pathname = dir.c_str();
    if (stat(pathname, &sb) == 0 && (S_IFDIR&sb.st_mode)) {
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
    if (stat(filename, &sb) == 0 && (S_IFLNK&sb.st_mode)) {
        return true;
    }
    return false;
}

void
create_directory(std::string dir)
{
    if (!directory_exists(dir)) {
        if (mkdir(dir.c_str(),0777) == -1) {
            std::perror("could not create directory");
            exit(EXIT_FAILURE);
        }
    }
}

void
rename_file(std::string from,std::string to)
{
    int rc = std::rename(from.c_str(),to.c_str()); 
    if(rc) { 
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
    struct dirent *next_file;
    auto folder = opendir(dir.c_str());
    while ( (next_file = readdir(folder)) != nullptr ) {
        std::string file_name = (const char*) next_file->d_name;
        if(file_name != "." && file_name != "..") {
            auto file_path = dir +"/"+file_name;
            remove_file(file_path);
        }
    }
}

#define ELPP_THREAD_SAFE

void
setup_logger(int argc,const char** argv)
{
    START_EASYLOGGINGPP(argc,argv);
    el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);
    el::Loggers::addFlag(el::LoggingFlag::HierarchicalLogging);
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Format, "%datetime %level:  %msg");
    std::string log_file = "rlz-log-"+std::to_string(getpid())+".log";
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Filename,log_file);
    el::Loggers::setLoggingLevel(el::Level::Fatal);    
}

} // end of util namespace
