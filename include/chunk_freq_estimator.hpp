#pragma once

#include "count_min_sketch.hpp"

template<uint32_t t_block_size>
class rabin_karp_hasher {
private:
	uint64_t prime = 2147483647ULL; // largest prime <2^31
	uint64_t nk;
	uint64_t num_chars = std::numeric_limits<uint8_t>::max() + 1ULL;
	void compute_nk() {
		nk = 1;
		for(size_t i=0;i<t_block_size;i++) {
			nk = (nk*num_chars)%prime;
		}
	}
public:
	rabin_karp_hasher() : hash(0) {
		compute_nk();
	}
	uint64_t hash;
	std::queue<uint8_t> cur_block;
	inline uint64_t update(uint64_t sym) {
		if(cur_block.size() != t_block_size) {
			hash = (hash*num_chars+sym)%prime;
		} else {
			auto tail = cur_block.front();
			cur_block.pop();
			// /* (1) scale up */
			hash = (hash * num_chars)%prime;
			// /* (2) add last sym */
			hash = (hash + sym)%prime;
			// /* (3) substract tail sym */
			hash = (hash + (prime - ((nk*tail)%prime)))%prime;
		}
		cur_block.push(sym);
		return hash;
	}
	template<class t_itr>
	uint64_t compute_hash(t_itr itr) {
		auto end = itr+t_block_size;
		uint64_t hash = 0;
		while(itr != end) {
			auto sym = *itr;
			hash = (hash*num_chars+sym)%prime;
			++itr;
		}
		return hash;
	}
};

struct chunk_info {
	uint64_t start;
	uint64_t hash;
	bool operator==(const chunk_info& ci) const {
		return hash == ci.hash;
	}
	bool operator<(const chunk_info& ci) const {
		return start < ci.start;
	}
};

namespace std
{
    template<>
    struct hash<chunk_info>
    {
        typedef chunk_info argument_type;
        typedef std::size_t result_type;
         result_type operator()(argument_type const& s) const
        {
            return s.hash;
        }
    };
}

template<
uint32_t t_chunk_size = 16,
class t_sketch = count_min_sketch<>
>
struct chunk_freq_estimator {
public:
	using sketch_type = t_sketch;
private:
	rabin_karp_hasher<t_chunk_size> rk_hash;
	sketch_type m_freq_sketch;
	uint64_t m_cur_offset = 0;
	uint64_t m_max_freq = 0;
public:
	uint32_t chunk_size = t_chunk_size;
	const sketch_type& sketch = m_freq_sketch;
	inline void update(uint8_t sym) {
		auto hash = rk_hash.update(sym);
		if(m_cur_offset >= t_chunk_size) {
			uint64_t freq = m_freq_sketch.update(hash);
			m_max_freq = std::max(freq,m_max_freq);
		}
		m_cur_offset++;
	}
	uint64_t estimate(uint64_t hash) const {
		return m_freq_sketch.estimate(hash);
	}
	uint64_t max_freq() const {
		return m_max_freq;
	}
};

template<
uint32_t t_chunk_size = 16,
uint32_t t_k = 1000,
class t_sketch = count_min_sketch<>
>
struct chunk_freq_estimator_topk {
public:
	using topk_sketch_type = sketch_topk<chunk_info,t_sketch>;
private:
	rabin_karp_hasher<t_chunk_size> rk_hash;
	topk_sketch_type m_freq_sketch = topk_sketch_type(t_k);
	uint64_t m_cur_offset = 0;
public:
	uint32_t chunk_size = t_chunk_size;
	const topk_sketch_type& sketch = m_freq_sketch;
	inline void update(uint8_t sym) {
		auto hash = rk_hash.update(sym);
		if(m_cur_offset >= (t_chunk_size -1) ) {
			chunk_info ci{m_cur_offset-(t_chunk_size-1),hash};
			m_freq_sketch.update(ci);
		}
		m_cur_offset++;
	}
	uint64_t estimate(uint64_t hash) const {
		chunk_info ci{0,hash};
		return m_freq_sketch.estimate(ci);
	}
	auto topk() -> decltype(m_freq_sketch.topk()) const {
		return m_freq_sketch.topk();
	}
	uint64_t topk_threshold() const {
		return m_freq_sketch.topk_threshold();
	}
};


struct var_chunk_info {
	uint64_t start;
	uint64_t len;
	uint64_t est;
	uint64_t pos_in_dict;
	var_chunk_info() : start(0), len(0), est(0), pos_in_dict(0) {}
	var_chunk_info(uint64_t a,uint64_t b,uint64_t e) : start(a), len(b), est(e) {}
	bool operator==(const var_chunk_info& ci) const {
		return start == ci.start;
	}
	bool operator<(const var_chunk_info& ci) const {
		return len < ci.len;
	}
	bool operator>(const var_chunk_info& ci) const {
		return len > ci.len;
	}
};

namespace std
{
    template<>
    struct hash<var_chunk_info>
    {
        typedef chunk_info argument_type;
        typedef std::size_t result_type;
        std::hash<uint64_t> hash_fn;
        result_type operator()(argument_type const& s) const
        {
            return hash_fn(s.start);
        }
    };
}

template<
uint32_t t_k = 1000
>
struct var_chunk_topk {
private:
	using boost_heap = boost::heap::fibonacci_heap<var_chunk_info, boost::heap::compare<std::greater<var_chunk_info>>>;
	boost_heap m_topk_pq;
	std::unordered_map<uint64_t,typename boost_heap::handle_type> m_topk_set;
public:
	inline void update(uint64_t start,uint64_t len,uint64_t est) {
		auto itr = m_topk_set.find(start);
		if(itr != m_topk_set.end()) {
			auto handle = itr->second;
			(*handle).len = len;
			(*handle).est += est;
			m_topk_pq.decrease(handle);
		} else {
			bool insert = false;
			if( m_topk_set.size() < t_k ) {
				insert = true;
			}
			
			if( m_topk_pq.size() && m_topk_pq.top().len < len ) {
				auto smallest = m_topk_pq.top();
				insert = true;
				m_topk_set.erase(smallest.start);
                m_topk_pq.pop();
			}

			if(insert) {
				auto handle = m_topk_pq.emplace(start,len,est);
				m_topk_set[start] = handle;
			}
		}
	}
	std::vector<var_chunk_info> topk() const {
		std::vector<var_chunk_info> cur_topk;
		for(const auto& pqi : m_topk_pq) {
			cur_topk.emplace_back(pqi);
		}
		std::sort(cur_topk.begin(),cur_topk.end(),std::less<var_chunk_info>());
		return cur_topk;
	}

};