#pragma once

#include <ratio>
#include <cmath>
#include <random>
#include <queue>
#include <unordered_map>

#include <boost/heap/fibonacci_heap.hpp>

#include <sdsl/int_vector.hpp>


struct hash_params {
	uint64_t a;
	uint64_t b;
};

template<
class t_epsilon = std::ratio<1, 20000>, // default settings ~ 2.5MB 
class t_delta = std::ratio<1, 1000>
>
struct count_min_sketch {
public:
    std::string type()
    {
        return "count_min_sketch-" + std::to_string(epsilon) + "-" + std::to_string(delta);
    }
private:
	using size_type = uint64_t;
	sdsl::int_vector<32> m_table;
	std::vector<hash_params> m_hash_params;
private:
	inline void pick_hash_params() {
		std::mt19937 gen(4711);
		std::uniform_int_distribution<uint32_t> param_dist(1, prime);
		for(size_t i=0;i<d;i++) {
			auto a = param_dist(gen); 
			auto b = param_dist(gen);
			m_hash_params.push_back({a,b});
		}
	}
	inline uint32_t compute_hash(uint64_t x,size_t row) const {
		uint64_t hash = (m_hash_params[row].a*x);// + m_hash_params[row].b;
		hash = ((hash >> 31) + hash) & prime;
		return (((uint32_t)hash) & w);
	}
public:
	double epsilon = (double) t_epsilon::num / (double) t_epsilon::den;
	double delta = (double) t_delta::num / (double) t_delta::den;
	uint64_t w_real = std::ceil(2.0 / epsilon);
	uint64_t w = (1ULL << (sdsl::bits::hi(w_real)+1ULL))-1ULL; // smallest power of 2 larger than w -1
	uint64_t d = std::ceil(std::log(1.0/delta)/std::log(2.0));
	uint64_t prime = 2147483647; // largest prime <2^32
	uint64_t total_count = 0;

	count_min_sketch() {
		m_table = sdsl::int_vector<32>((w+1)*d);
		pick_hash_params();
	} 
	uint64_t update(uint64_t item,size_t count = 1) {
		total_count += count;
		uint64_t new_est = std::numeric_limits<uint64_t>::max();
		for(size_t i=0;i<d;i++) {
			auto row_offset = compute_hash(item,i);
			auto col_offset = (w+1)*i;
			uint64_t new_count = m_table[row_offset+col_offset] + count;
			m_table[row_offset+col_offset] = new_count;
			new_est = std::min(new_est,new_count);
		}
		return new_est;
	}
	uint64_t estimate(uint64_t item) const {
		uint64_t est = std::numeric_limits<uint64_t>::max();
		for(size_t i=0;i<d;i++) {
			auto row_offset = compute_hash(item,i);
			auto col_offset = (w+1)*i;
			uint64_t val = m_table[row_offset+col_offset];
			est = std::min(est,val);
		}
		return est;
	}
	uint64_t size_in_bytes() const {
		uint64_t bytes = 0;
		bytes += sdsl::size_in_bytes(m_table);
		bytes += d*sizeof(hash_params);
		return bytes;
	}
	double estimation_error() const {
		return epsilon * total_count;
	}
	double estimation_probability() const {
		return 1.0 - delta;
	}

	double noise_estimate() const {
		uint64_t noise_overall = 0;
		for(size_t j=0;j<d;j++) {
			uint64_t noise = std::numeric_limits<uint64_t>::max();
			for(size_t i=0;i<(w+1);i++) {
				uint64_t elem = m_table[i];
				noise = std::min(elem,noise);
			}
			noise_overall = std::max(noise_overall,noise);
		}
		return noise_overall - 1;
	}

    size_type serialize(std::ostream& out,sdsl::structure_tree_node* v=nullptr,std::string name = "") const {
	    sdsl::structure_tree_node* child = sdsl::structure_tree::add_child(v, name, sdsl::util::class_name(*this));
	    size_type written_bytes = 0;
	    written_bytes += m_table.serialize(out, child, "m_table");
	    uint64_t num_hash_params = m_hash_params.size();
	    sdsl::int_vector<64> hparams(num_hash_params*2);
	    for(size_t i=0;i<m_hash_params.size();i++) {
	    	hparams[2*i] = m_hash_params[i].a;
	    	hparams[2*i+1] = m_hash_params[i].b;
	    }
	    written_bytes += hparams.serialize(out, child, "hash_params");
	    written_bytes += sdsl::write_member(total_count,out,child,"total_count");
	    sdsl::structure_tree::add_size(child, written_bytes);
    	return written_bytes;
    }

    void load(std::istream& in) {
    	m_table.load(in);
    	sdsl::int_vector<64> hparams;
    	hparams.load(in);
    	uint64_t num_hash_params = hparams.size()/2;
    	m_hash_params.resize(num_hash_params);
    	for(size_t i=0;i<hparams.size();i+=2) {
    		size_t j = i>>1;
    		m_hash_params[j].a = hparams[i];
    		m_hash_params[j].b = hparams[i+1];
    	}
    	sdsl::read_member(total_count,in);
    }
};

template<
class T,
class t_cms = count_min_sketch<>
>
struct sketch_topk {
	using size_type = uint64_t;
    std::string type()
    {
        return "sketch_topk-" + std::to_string(m_k) + "-" + m_sketch.type();
    }
private:
	struct topk_item {
		T item;
		uint64_t estimate;
		topk_item(T& i,uint64_t e) : item(i), estimate(e) {};
		bool operator<(const topk_item& i) const {
			return estimate < i.estimate;
		}
		bool operator>(const topk_item& i) const {
			return estimate > i.estimate;
		}
		bool operator==(const T& i) {
			return item == i;
		}
	};
	t_cms m_sketch;
	using boost_heap = boost::heap::fibonacci_heap<topk_item, boost::heap::compare<std::greater<topk_item>>>;
	boost_heap m_topk_pq;
	std::unordered_map<T,typename boost_heap::handle_type> m_topk_set;
	std::hash<T> hash_fn;
	uint64_t m_k;
private:
	inline void update_topk(T& item,uint64_t estimate) {
		if( m_topk_set.size() < m_k ) {
			auto itr = m_topk_set.find(item);
			if(itr == m_topk_set.end()) {
				auto handle = m_topk_pq.emplace(item,estimate);
				m_topk_set[item] = handle;
			} else {
				auto handle = itr->second;
				(*handle).estimate = estimate;
				m_topk_pq.decrease(handle);
			}
		} else {
			auto itr = m_topk_set.find(item);
			if(itr != m_topk_set.end()) {
				auto handle = itr->second;
				(*handle).estimate = estimate;
				m_topk_pq.decrease(handle);
			} else {
				// check against smallest in pq
				if( estimate != 1 && m_topk_pq.top().estimate < estimate ) {
					auto cur_smallest = m_topk_pq.top();
					m_topk_set.erase(cur_smallest.item);
                    m_topk_pq.pop();
					auto handle = m_topk_pq.emplace(item,estimate);
					m_topk_set[item] = handle;
				}
			}
		}
	}
public:
	const t_cms& sketch = m_sketch;
	const uint64_t& d = sketch.d;
	const uint64_t& w = sketch.w;
	sketch_topk(size_t k) :m_k(k) {

	}
	uint64_t update(T& item,size_t count = 1) {
		uint64_t hash = hash_fn(item);
		auto estimate = m_sketch.update(hash,count);
		update_topk(item,estimate);
		return estimate;
	}
	uint64_t estimate(T& item) const {
		uint64_t hash = hash_fn(item);
		return m_sketch.estimate(hash);
	}
	uint64_t size_in_bytes() const {
		return m_sketch.size_in_bytes() + (2*m_k*sizeof(topk_item));
	}
	std::vector<std::pair<uint64_t,T>> topk() const {
		std::vector<std::pair<uint64_t,T>> cur_topk;
		for(const auto& pqi : m_topk_pq) {
			cur_topk.emplace_back(pqi.estimate,pqi.item);
		}
		std::sort(cur_topk.begin(),cur_topk.end(),std::greater<std::pair<uint64_t,T>>());
		return cur_topk;
	}
	uint64_t topk_threshold() const {
		return m_topk_pq.top().estimate;
	}
	double estimation_error() const {
		return sketch.estimation_error();
	}
	double estimation_probability() const {
		return sketch.estimation_probability();
	}

	double noise_estimate() const {
		return sketch.noise_estimate();
	}

    size_type serialize(std::ostream& out,sdsl::structure_tree_node* v=nullptr,std::string name = "") const {
	    sdsl::structure_tree_node* child = sdsl::structure_tree::add_child(v, name, sdsl::util::class_name(*this));
	    size_type written_bytes = 0;
	    written_bytes += m_sketch.serialize(out, child, "m_sketch");

	    sdsl::int_vector<64> ests(m_topk_set.size());
	    std::vector<T> items(m_topk_set.size());
	    size_t nitems = 0;
	    for(const auto& tkitems : m_topk_set) {
	    	ests[nitems] = (*tkitems.second).estimate;
	    	items[nitems] = tkitems.first;
	    	nitems++;
	    }
	    written_bytes += ests.serialize(out,child,"topk_estimates");
	    written_bytes += sdsl::serialize(items,out,child,"topk_items");
	    sdsl::structure_tree::add_size(child, written_bytes);
    	return written_bytes;
    }

    void load(std::istream& in) {
    	m_sketch.load(in);
    	sdsl::int_vector<64> ests;
    	ests.load(in);
    	std::vector<T> items;
    	sdsl::load(items,in);
    	for(size_t i=0;i<ests.size();i++) {
    		auto cest = ests[i];
    		auto citem = items[i];
    		auto handle = m_topk_pq.emplace(citem,cest);
    		m_topk_set[citem] = handle;
    	}
    }

};


