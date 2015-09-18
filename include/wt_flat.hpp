/* sdsl - succinct data structures library
    Copyright (C) 2009 Simon Gog

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see http://www.gnu.org/licenses/ .
*/
/*! \file wt_int.hpp
    \brief wt_int.hpp contains a specialized class for a wavelet tree of a
           sequence of the numbers. This wavelet tree class takes
           less memory than the wt_pc class for large alphabets.
    \author Simon Gog, Shanika Kuruppu
*/
#ifndef INCLUDED_SDSL_WT_FLAT
#define INCLUDED_SDSL_WT_FLAT

#include "sdsl/sdsl_concepts.hpp"
#include "sdsl/int_vector.hpp"
#include "sdsl/sdsl_concepts.hpp"
#include <array>

//! A wavelet tree class for integer sequences.
/*!
 *    \par Space complexity
 *        \f$\Order{n\log|\Sigma|}\f$ bits, where \f$n\f$ is the size of the vector the wavelet tree was build for.
 *
 *  \tparam t_bitvector   Type of the bitvector used for representing the wavelet tree.
 *  \tparam t_rank        Type of the support structure for rank on pattern `1`.
 *  \tparam t_select      Type of the support structure for select on pattern `1`.
 *  \tparam t_select_zero Type of the support structure for select on pattern `0`.
 *
 *   @ingroup wt
 */
template<class t_bitvector   = sdsl::bit_vector,
         class t_rank        = typename t_bitvector::rank_1_type,
         class t_select      = typename t_bitvector::select_1_type,
         class t_select_zero = typename t_bitvector::select_0_type>
class wt_flat
{
    public:
        typedef sdsl::int_vector<>::size_type               size_type;
        typedef sdsl::int_vector<>::value_type              value_type;
        typedef typename t_bitvector::difference_type 		difference_type;
        typedef t_bitvector                           		bit_vector_type;
        typedef t_rank                                		rank_1_type;
        typedef t_select                              		select_1_type;
        typedef t_select_zero                         		select_0_type;
        typedef sdsl::wt_tag                                index_category;
        typedef sdsl::byte_alphabet_tag                     alphabet_category;

    protected:

        size_type              m_size  = 0;
        size_type              m_sigma = 0;    //<- \f$ |\Sigma| \f$
        std::array<bit_vector_type,256> m_bvs;
        std::array<rank_1_type,256> m_bvs_rank;
    public:
        const size_type&       sigma = m_sigma;
        //! Default constructor
        wt_flat() = default;

        template<uint8_t int_width>
        wt_flat(sdsl::int_vector_buffer<int_width>& buf, size_type size,
               uint32_t max_level=0) : m_size(size) {
            if (0 == m_size)
                return;
            size_type n = buf.size();  // set n

            sdsl::int_vector<int_width> T(n);
            for(size_t i=0;i<n;i++) T[i] = buf[i];

            /* make at most sigma passes over the text */
            std::array<uint64_t,256> C{0};
            for(size_t i=0;i<n;i++) {
            	C[T[i]]++;
            }
            for(size_t i=0;i<256;i++) {
            	if(C[i]!=0) {
            		std::cout << "bv for sym " << i << std::endl;
            		sdsl::bit_vector bv(n);
            		for(size_t j=0;j<n;j++) if(T[j] == i) bv[j] = 1;
            		m_bvs[i] = std::move(bit_vector_type(bv));
            		m_bvs_rank[i] = std::move(rank_1_type(& m_bvs[i]));
            		m_sigma++;
            	}
            }
        }

        //! Copy constructor
        wt_flat(const wt_flat& wt) = default;
        //! Copy constructor
        wt_flat(wt_flat&& wt) = default;
        //! Assignment operator
        wt_flat& operator=(const wt_flat& wt) = default;

        //! Assignment move operator
        wt_flat& operator=(wt_flat&& wt) = default;

        //! Returns the size of the original vector.
        size_type size()const {
            return m_size;
        }

        //! Returns whether the wavelet tree contains no data.
        bool empty()const {
            return m_size == 0;
        }

        //! Recovers the i-th symbol of the original vector.
        /*! \param i The index of the symbol in the original vector.
         *  \returns The i-th symbol of the original vector.
         *  \par Precondition
         *       \f$ i < size() \f$
         */
        value_type operator[](size_type i)const {
        	static_assert(true,"does not work!");
            return 0;
        };

        void swap(wt_flat& wt) {
            if (this != &wt) {
                std::swap(m_size, wt.m_size);
                std::swap(m_sigma,  wt.m_sigma);
                m_bvs.swap(wt.m_bvs);
                for(size_t i=0;i<256;i++) {
                	sdsl::util::swap_support(m_bvs_rank[i], wt.m_bvs_rank[i], &m_bvs[i], &(wt.m_bvs[i]));
                }
            }
        }

        //! Calculates how many symbols c are in the prefix [0..i-1] of the supported vector.
        /*!
         *  \param i The exclusive index of the prefix range [0..i-1], so \f$i\in[0..size()]\f$.
         *  \param c The symbol to count the occurrences in the prefix.
         *    \returns The number of occurrences of symbol c in the prefix [0..i-1] of the supported vector.
         *  \par Time complexity
         *       \f$ \Order{\log |\Sigma|} \f$
         *  \par Precondition
         *       \f$ i \leq size() \f$
         */
        size_type rank(size_type i, value_type c)const {
            assert(i <= size());
            if(m_bvs[c].size()==0) return 0;
            return m_bvs_rank[c].rank(i);
        };

        std::pair<size_type,size_type> double_rank(size_type i,size_type j, value_type c)const {
            assert(i <= size());
            if(m_bvs[c].size()==0) return {0,0};
            return {m_bvs_rank[c].rank(i),m_bvs_rank[c].rank(j)};
        };

        //! Calculates how many occurrences of symbol wt[i] are in the prefix [0..i-1] of the original sequence.
        /*!
         *  \param i The index of the symbol.
         *  \return  Pair (rank(wt[i],i),wt[i])
         *  \par Precondition
         *       \f$ i < size() \f$
         */
        std::pair<size_type, value_type>
        inverse_select(size_type i)const {
            static_assert(true,"does not work!");
            return {0,0};
        }

        //! Calculates the i-th occurrence of the symbol c in the supported vector.
        /*!
         *  \param i The i-th occurrence.
         *  \param c The symbol c.
         *  \par Time complexity
         *       \f$ \Order{\log |\Sigma|} \f$
         *  \par Precondition
         *       \f$ 1 \leq i \leq rank(size(), c) \f$
         */
        size_type select(size_type i, value_type c) const {
        	static_assert(true,"does not work!");
        	return 0;
        };

        //! Serializes the data structure into the given ostream
        size_type serialize(std::ostream& out, sdsl::structure_tree_node* v=nullptr, std::string name="")const {
            sdsl::structure_tree_node* child = sdsl::structure_tree::add_child(v, name, sdsl::util::class_name(*this));
            size_type written_bytes = 0;
            written_bytes += sdsl::write_member(m_size, out, child, "size");
            written_bytes += sdsl::write_member(m_sigma, out, child, "sigma");
            for(size_t i=0;i<256;i++) {
            	written_bytes += m_bvs[i].serialize(out,child,"bv");
            	written_bytes += m_bvs_rank[i].serialize(out,child,"rank_bv");
            }
            sdsl::structure_tree::add_size(child, written_bytes);
            return written_bytes;
        }

        //! Loads the data structure from the given istream.
        void load(std::istream& in) {
            sdsl::read_member(m_size, in);
            sdsl::read_member(m_sigma, in);
            for(size_t i=0;i<256;i++) {
            	m_bvs[i].load(in);
            	m_bvs_rank[i].load(in,&m_bvs[i]);
            }
        }
};

#endif
