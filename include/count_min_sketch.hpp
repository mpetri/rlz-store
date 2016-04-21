#pragma once

#include <ratio>
#include <cmath>
#include <random>
#include <queue>
#include <unordered_map>

#include <sdsl/int_vector.hpp>

struct hash_params {
    uint64_t a;
    uint64_t b;
};

template <class t_epsilon = std::ratio<1, 20000>, // default settings ~ 2.5MB
    class t_delta = std::ratio<1, 1000> >
struct count_min_sketch {
public:
    static std::string type()
    {
        return "count_min_sketch-" + std::to_string(t_epsilon::den) + "-" + std::to_string(t_delta::den);
    }

private:
    using size_type = uint64_t;
    sdsl::int_vector<32> m_table;
    std::vector<hash_params> m_hash_params;
    uint64_t m_total_count = 0;

private:
    inline void pick_hash_params()
    {
        std::mt19937 gen(4711);
        std::uniform_int_distribution<uint32_t> param_dist(1, prime);
        for (size_t i = 0; i < d; i++) {
            auto a = param_dist(gen);
            auto b = param_dist(gen);
            m_hash_params.push_back({ a, b });
        }
    }
    inline uint32_t compute_hash(uint64_t x, size_t row) const
    {
        uint64_t hash = (m_hash_params[row].a * x); // + m_hash_params[row].b;
        hash = ((hash >> 31) + hash) & prime;
        return (((uint32_t)hash) & w);
    }

public:
    const double epsilon = (double)t_epsilon::num / (double)t_epsilon::den;
    const double delta = (double)t_delta::num / (double)t_delta::den;
    const uint64_t w_real = std::ceil(2.0 / epsilon);
    const uint64_t w = (1ULL << (sdsl::bits::hi(w_real) + 1ULL)) - 1ULL; // smallest power of 2 larger than w -1
    const uint64_t d = std::ceil(std::log(1.0 / delta) / std::log(2.0));
    const uint64_t prime = 2147483647; // largest prime <2^32
public:
    count_min_sketch<t_epsilon, t_delta>& operator=(const count_min_sketch<t_epsilon, t_delta>& cms)
    {
        m_table = cms.m_table;
        m_hash_params = cms.m_hash_params;
        m_total_count = cms.m_total_count;
        return *this;
    }
    count_min_sketch<t_epsilon, t_delta>& operator=(count_min_sketch<t_epsilon, t_delta>&& cms)
    {
        m_table = std::move(cms.m_table);
        m_hash_params = std::move(cms.m_hash_params);
        m_total_count = cms.m_total_count;
        return *this;
    }
    count_min_sketch(count_min_sketch<t_epsilon, t_delta>&& cms)
    {
        m_table = std::move(cms.m_table);
        m_hash_params = std::move(cms.m_hash_params);
        m_total_count = cms.m_total_count;
    }
    count_min_sketch(const count_min_sketch<t_epsilon, t_delta>& cms)
    {
        m_table = cms.m_table;
        m_hash_params = cms.m_hash_params;
        m_total_count = cms.m_total_count;
    }

public:
    count_min_sketch()
    {
        m_table = sdsl::int_vector<32>((w + 1) * d);
        pick_hash_params();
    }
    uint64_t update(uint64_t item, size_t count = 1)
    {
        m_total_count += count;
        uint64_t new_est = std::numeric_limits<uint64_t>::max();
        for (size_t i = 0; i < d; i++) {
            auto row_offset = compute_hash(item, i);
            auto col_offset = (w + 1) * i;
            uint64_t new_count = m_table[row_offset + col_offset] + count;
            m_table[row_offset + col_offset] = new_count;
            new_est = std::min(new_est, new_count);
        }
        return new_est;
    }
    uint64_t estimate(uint64_t item) const
    {
        uint64_t est = std::numeric_limits<uint64_t>::max();
        for (size_t i = 0; i < d; i++) {
            auto row_offset = compute_hash(item, i);
            auto col_offset = (w + 1) * i;
            uint64_t val = m_table[row_offset + col_offset];
            est = std::min(est, val);
        }
        return est;
    }
    uint64_t size_in_bytes() const
    {
        uint64_t bytes = 0;
        bytes += sdsl::size_in_bytes(m_table);
        bytes += d * sizeof(hash_params);
        return bytes;
    }
    double estimation_error() const
    {
        return (double)(2 * m_total_count / (w + 1));
    }
    double estimation_probability() const
    {
        return 1.0 - (double)1 / (std::pow(2, d));
    }

    uint64_t total_count() const
    {
        return m_total_count;
    }

    double noise_estimate() const
    {
        uint64_t noise_overall = 0;
        for (size_t j = 0; j < d; j++) {
            uint64_t noise = std::numeric_limits<uint64_t>::max();
            for (size_t i = 0; i < (w + 1); i++) {
                uint64_t elem = m_table[i];
                noise = std::min(elem, noise);
            }
            noise_overall = std::max(noise_overall, noise);
        }
        return noise_overall - 1;
    }

    size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = nullptr, std::string name = "") const
    {
        sdsl::structure_tree_node* child = sdsl::structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += m_table.serialize(out, child, "m_table");
        uint64_t num_hash_params = m_hash_params.size();
        sdsl::int_vector<64> hparams(num_hash_params * 2);
        for (size_t i = 0; i < m_hash_params.size(); i++) {
            hparams[2 * i] = m_hash_params[i].a;
            hparams[2 * i + 1] = m_hash_params[i].b;
        }
        written_bytes += hparams.serialize(out, child, "hash_params");
        written_bytes += sdsl::write_member(m_total_count, out, child, "total_count");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    void load(std::istream& in)
    {
        m_table.load(in);
        sdsl::int_vector<64> hparams;
        hparams.load(in);
        uint64_t num_hash_params = hparams.size() / 2;
        m_hash_params.resize(num_hash_params);
        for (size_t i = 0; i < hparams.size(); i += 2) {
            size_t j = i >> 1;
            m_hash_params[j].a = hparams[i];
            m_hash_params[j].b = hparams[i + 1];
        }
        sdsl::read_member(m_total_count, in);
    }

    void merge(const count_min_sketch<t_epsilon, t_delta>& cms)
    {
        for (size_t i = 0; i < m_table.size(); i++) {
            m_table[i] += cms.m_table[i];
        }
        m_total_count += cms.m_total_count;
    }
};

