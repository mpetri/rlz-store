#pragma once

#include "logging.hpp"

template<class T>
class cover_pq {
private:
	uint64_t m_top = 0;
	uint64_t m_size = 0;
	std::vector<std::vector<T>> m_data;
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
	void emplace(T&& item) {
		if(item.weight() >= m_top) {
			m_data.resize(item.weight()+1);
			m_top = item.weight();
		}
		m_data[item.weight()].emplace_back(item);
		m_size++;
	}
	void push(const T& item) {
		if(item.weight() >= m_top) {
			m_data.resize(item.weight()+1);
			m_top = item.weight();
		}
		m_data[item.weight()].push_back(item);
		m_size++;
	}
	T& top() {
		return m_data[m_top].back(); 
	}

	void sift_top() {
		auto new_weight = top().weight();
		auto cur_top_itr = m_data[m_top].end()-1;
		std::move(cur_top_itr,m_data[m_top].end(),std::back_inserter(m_data[new_weight]));
		m_data[m_top].resize(m_data[m_top].size()-1);
		update_top_ptr();
	}

	T&& top_and_pop() {
		T item = m_data[m_top].back();
		pop();
		return std::move(item);
	}

	void pop() {
		if(m_size != 0) {
			auto cur_top_itr = m_data[m_top].end()-1;
			m_data[m_top].erase( cur_top_itr ); // remove the end!
			update_top_ptr();
			m_size--;
		}
	}

	uint64_t size() const {
		return m_size;
	}

	bool empty() const {
		return size() == 0;
	}
};