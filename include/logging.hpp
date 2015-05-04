#pragma once

#define ELPP_THREAD_SAFE

#include "easylogging++.h"

void
setup_logger(int argc, const char** argv, bool log_stdout = true)
{
    START_EASYLOGGINGPP(argc, argv);
    el::Loggers::addFlag(el::LoggingFlag::ColoredTerminalOutput);
    el::Loggers::addFlag(el::LoggingFlag::HierarchicalLogging);
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Format, "%datetime %level:  %msg");
    if (!log_stdout)
        el::Loggers::reconfigureAllLoggers(el::ConfigurationType::ToStandardOutput, "false");
    std::string log_file = "logs/rlz-log-" + std::to_string(getpid()) + ".log";
    el::Loggers::reconfigureAllLoggers(el::ConfigurationType::Filename, log_file);
    el::Loggers::setLoggingLevel(el::Level::Fatal);
}

