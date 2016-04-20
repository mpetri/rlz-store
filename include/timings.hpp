#pragma once

#include <array>
#include <chrono>

using namespace std::chrono;
using watch = std::chrono::high_resolution_clock;

enum class timer_type {
    FindFactor = 0,
    EncodeBlock,
    PickOffset
};
const uint64_t num_timer_types = 3;

std::string timer_type_to_str(int type)
{
    timer_type t = static_cast<timer_type>(type);
    switch (t) {
    case timer_type::FindFactor:
        return "FindFactor";
    case timer_type::EncodeBlock:
        return "EncodeBlock";
    case timer_type::PickOffset:
        return "PickOffset";
    }
    return "SHOULD NEVER HAPPEN";
}

struct bench_data {
    std::array<uint64_t, num_timer_types> num_calls{ { 0 } };
    std::array<nanoseconds, num_timer_types> total_time{ { nanoseconds::zero() } };
};

struct lm_timer {
    timer_type type;
    watch::time_point start;
    bench_data& bd;
    lm_timer(timer_type t, bench_data& _bd)
        : type(t)
        , bd(_bd)
    {
        start = watch::now();
    }
    ~lm_timer()
    {
        auto stop = watch::now();
        auto time_spent = stop - start;
        /* update stats */
        bd.num_calls[static_cast<int>(type)]++;
        bd.total_time[static_cast<int>(type)] += time_spent;
    }
};

struct lm_bench {
private:
    static bench_data& data()
    {
        static bench_data bd;
        return bd;
    }

public:
    static lm_timer bench(timer_type t)
    {
        auto& bd = data();
        return lm_timer(t, bd);
    }
    static void reset()
    {
        auto& d = data();
        d = bench_data();
    }
    static void print()
    {
        auto& d = data();
        LOG(INFO) << "TIMINGS";
        for (size_t i = 0; i < num_timer_types; i++) {
            LOG(INFO) << std::setw(19) << timer_type_to_str(i)
                      << " Calls=" << std::setw(11) << d.num_calls[i]
                      << " Total=" << std::setw(11) << std::setprecision(6)
                      << duration_cast<milliseconds>(d.total_time[i]).count() / 1000.0f << " sec"
                      << " Avg=" << std::setw(11)
                      << d.total_time[i].count() / (d.num_calls[i] == 0 ? 1 : d.num_calls[i]) << " ns";
        }
    }
};