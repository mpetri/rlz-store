#pragma once

#include "logging.hpp"

template<uint32_t t_max_weight>
struct block_cover {
	uint64_t id;
	uint32_t w;
	uint32_t contents[t_max_weight];
	uint64_t weight() const {
		return w;
	}
	void add(uint32_t v) {
		contents[w++] = v;
	}
};

template<uint32_t t_max_weight>
class cover_pq {
public:
	using value_type = block_cover<t_max_weight>;
private:
	uint64_t m_top = 0;
	uint64_t m_size = 0;
	uint64_t m_min_weight = 1;
	std::vector<std::vector<value_type>> m_data;
	std::FILE* m_disk_buffer = nullptr;
	uint64_t m_max_size = 0;
	uint64_t m_buffered = 0;
	uint64_t m_in_memory = 0;
	uint64_t m_move_to_disk_amount = 50000;
private:
	void update_top_ptr() {
		if( m_data[m_top].empty() ) {
			for(size_t new_top = m_top;new_top != 0;new_top--) {
				if(! m_data[new_top].empty() ) {
					m_top = new_top;
					return;
				}
			}
		}
	}
public:
	cover_pq(uint64_t max_size = 0,uint64_t min_weight = 1,uint64_t move_to_disk_amount = 0) {
		m_max_size = max_size;
		m_min_weight = min_weight;
		m_disk_buffer = std::tmpfile();
		m_move_to_disk_amount = move_to_disk_amount;
	}
	void emplace(value_type&& item) {
		if(item.weight() < m_min_weight) return;
		if(item.weight() >= m_top) {
			m_data.resize(item.weight()+1);
			m_top = item.weight();
		}
		m_data[item.weight()].emplace_back(item);
		m_size++;
		m_in_memory++;
		compact();
	}
	void push(const value_type& item) {
		if(item.weight() < m_min_weight) return;
		if(item.weight() >= m_top) {
			m_data.resize(item.weight()+1);
			m_top = item.weight();
		}
		m_data[item.weight()].push_back(item);
		m_size++;
		m_in_memory++;
		compact();
	}
	value_type& top() {
		return m_data[m_top].back(); 
	}

	void sift_top() {
		auto new_weight = top().weight();
		auto cur_top_itr = m_data[m_top].end()-1;
		std::move(cur_top_itr,m_data[m_top].end(),std::back_inserter(m_data[new_weight]));
		m_data[m_top].resize(m_data[m_top].size()-1);
		update_top_ptr();
	}

	value_type&& top_and_pop() {
		value_type item = m_data[m_top].back();
		pop();
		return std::move(item);
	}

	void pop() {
		if(m_size != 0) {
			auto cur_top_itr = m_data[m_top].end()-1;
			m_data[m_top].erase( cur_top_itr ); // remove the end!
			update_top_ptr();
			m_size--;
			m_in_memory--;
		}
	}

	uint64_t size() const {
		return m_size;
	}

	bool empty() const {
		return size() == 0;
	}

    void compact() {
    	if(m_max_size != 0 && m_in_memory > m_max_size) {
	    	size_t moved_to_disk = 0;
	    	for(size_t i=0;i<m_data.size();i++) {
	    		auto& list = m_data[i];
	    		if(list.size()) {
	    			auto ldata = list.data();
	    			std::fwrite((char*)ldata, sizeof(value_type), sizeof(list.size()), m_disk_buffer);
	    			moved_to_disk += list.size();
	    			m_buffered += list.size();
	    			list.clear();
	    			m_in_memory -= list.size();
	    		}
	    		if(moved_to_disk >= m_move_to_disk_amount) break;
	    	}
	    }
    }

    void expand() {

    }

    uint64_t min_weight() const {
    	return m_min_weight;
    }

    void print_stats() {
    	for(size_t i=0;i<m_data.size();i++) {
    		if(!m_data[i].empty()) LOG(INFO) << "(" << i << ") = " << m_data[i].size();
    	}
    }
};
