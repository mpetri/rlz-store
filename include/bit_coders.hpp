#pragma once

#include "sdsl/int_vector.hpp"
#include "bit_streams.hpp"
#include "zlib.h"


#include "easylogging++.h"


namespace coder
{

struct elias_gamma {
    static std::string type() {
        return "elias_gamma";
    }

    template<typename T>
    static inline uint64_t encoded_length(const T& x)
    {
        uint8_t len_1 = sdsl::bits::hi(x);
        return len_1*2+1;
    }
    template<class t_bit_ostream,typename T>
    static void encode_check_size(t_bit_ostream& os,const T& x)
    {
        static_assert(std::numeric_limits<T>::is_signed == false,"can only encode unsigned integers");
        uint8_t len_1 = sdsl::bits::hi(x);
        os.expand_if_needed(len_1*2+1);
        os.put_unary_no_size_check(len_1);
        if (len_1) {
            os.put_int_no_size_check(x, len_1);
        }
    }
    template<class t_bit_ostream,typename T>
    static void encode(t_bit_ostream& os,const T& x)
    {
        static_assert(std::numeric_limits<T>::is_signed == false,"can only encode unsigned integers");
        uint8_t len_1 = sdsl::bits::hi(x);
        os.put_unary_no_size_check(len_1);
        if (len_1) {
            os.put_int_no_size_check(x, len_1);
        }
    }
    template<class t_bit_ostream,typename t_itr>
    static void encode(t_bit_ostream& os,t_itr begin,t_itr end)
    {
        uint64_t bits_required = 0;
        auto tmp = begin;
        while (tmp != end) {
            bits_required += encoded_length(*tmp);
            ++tmp;
        }
        os.expand_if_needed(bits_required);
        tmp = begin;
        while (tmp != end) {
            encode(os,*tmp);
            ++tmp;
        }
    }
    template<class t_bit_istream>
    static uint64_t decode(const t_bit_istream& is)
    {
        uint64_t x;
        auto len = is.get_unary();
        x = 1ULL << len;
        if (len) x |= is.get_int(len);
        return x;
    }
    template<class t_bit_istream,typename t_itr>
    static void decode(const t_bit_istream& is,t_itr it,size_t n)
    {
        for (size_t i=0; i<n; i++) {
            *it = decode(is);
            ++it;
        }
    }
};

struct elias_delta {
    static std::string type() {
        return "elias_delta";
    }

    template<typename T>
    static inline uint64_t encoded_length(const T& x)
    {
        uint8_t len_1 = sdsl::bits::hi(x);
        return len_1 + (sdsl::bits::hi(len_1+1)<<1) + 1;
    }
    template<class t_bit_ostream,typename T>
    static void encode_check_size(t_bit_ostream& os,const T& x)
    {
        static_assert(std::numeric_limits<T>::is_signed == false,"can only encode unsigned integers");
        // (number of sdsl::bits to represent x)
        uint8_t len         = sdsl::bits::hi(x)+1;
        // (number of sdsl::bits to represent the length of x) -1
        uint8_t len_1_len   = sdsl::bits::hi(len);
        uint8_t bits = len  + (len_1_len<<1) + 1;
        os.expand_if_needed(bits);

        // Write unary representation for the length of the length of x
        os.put_unary_no_size_check(len_1_len);
        if (len_1_len) {
            os.put_int_no_size_check(len, len_1_len);
            os.put_int_no_size_check(x, len-1);
        }
    }
    template<class t_bit_ostream,typename T>
    static void encode(t_bit_ostream& os,const T& x)
    {
        static_assert(std::numeric_limits<T>::is_signed == false,"can only encode unsigned integers");
        // (number of sdsl::bits to represent x)
        uint8_t len         = sdsl::bits::hi(x)+1;
        // (number of sdsl::bits to represent the length of x) -1
        uint8_t len_1_len   = sdsl::bits::hi(len);
        // Write unary representation for the length of the length of x
        os.put_unary_no_size_check(len_1_len);
        if (len_1_len) {
            os.put_int_no_size_check(len, len_1_len);
            os.put_int_no_size_check(x, len-1);
        }
    }
    template<class t_bit_ostream,typename t_itr>
    static void encode(t_bit_ostream& os,t_itr begin,t_itr end)
    {
        auto tmp = begin;
        uint64_t len = 0;
        while (tmp != end) {
            len += encoded_length(*tmp);
            ++tmp;
        }
        os.expand_if_needed(len);
        tmp = begin;
        while (tmp != end) {
            encode(os,*tmp);
            ++tmp;
        }
    }
    template<class t_bit_istream>
    static uint64_t decode(const t_bit_istream& is)
    {
        uint64_t x = 1;
        auto len_1_len = is.get_unary();
        if (len_1_len) {
            auto len_1 = is.get_int(len_1_len);
            auto len = len_1 + (1ULL << len_1_len);
            x = is.get_int(len-1);
            x = x + (len-1<64) * (1ULL << (len-1));
        }
        return x;
    }
    template<class t_bit_istream,typename t_itr>
    static void decode(const t_bit_istream& is,t_itr it,size_t n)
    {
        for (size_t i=0; i<n; i++) {
            *it = decode(is);
            ++it;
        }
    }
};

struct vbyte {
    static std::string type() {
        return "vbyte";
    }

    template<typename T>
    static inline uint64_t encoded_length(const T& x)
    {
        const uint8_t vbyte_len[64] = {1,1,1,1,1,1,1,2,2,2,2,2,2,2,3,3,3,3,3,3,3,4,4,4,4,4,4,4,
                                       5,5,5,5,5,5,5,6,6,6,6,6,6,6,7,7,7,7,7,7,7,8,8,8,8,8,8,8
                                      };
        return 8*vbyte_len[sdsl::bits::hi(x)+1];
    }
    template<class t_bit_ostream,typename T>
    static void encode_check_size(t_bit_ostream& os,T y)
    {
        static_assert(std::numeric_limits<T>::is_signed == false,"can only encode unsigned integers");
        os.expand_if_needed(encoded_length(y));
        uint64_t x = y;
        uint8_t w = x & 0x7F;
        x >>= 7;
        while (x > 0) {
            w |= 0x80; // mark overflow bit
            os.put_int_no_size_check(w,8);
            w = x & 0x7F;
            x >>= 7;
        }
        os.put_int_no_size_check(w,8);
    }
    template<class t_bit_ostream,typename T>
    static void encode(t_bit_ostream& os,T y)
    {
        static_assert(std::numeric_limits<T>::is_signed == false,"can only encode unsigned integers");
        uint64_t x = y;
        uint8_t w = x & 0x7F;
        x >>= 7;
        while (x > 0) {
            w |= 0x80; // mark overflow bit
            os.put_int_no_size_check(w,8);
            w = x & 0x7F;
            x >>= 7;
        }
        os.put_int_no_size_check(w,8);
    }
    template<class t_bit_ostream,typename t_itr>
    static void encode(t_bit_ostream& os,t_itr begin,t_itr end)
    {
        uint64_t bits_required = 0;
        auto tmp = begin;
        while (tmp != end) {
            bits_required += encoded_length(*tmp);
            ++tmp;
        }
        os.expand_if_needed(bits_required);
        tmp = begin;
        while (tmp != end) {
            encode(os,*tmp);
            ++tmp;
        }
    }
    template<class t_bit_istream>
    static uint64_t decode(const t_bit_istream& is)
    {
        uint64_t ww=0;
        uint8_t w=0;
        uint64_t shift=0;
        do {
            w = is.get_int(8);
            ww |= (((uint64_t)(w&0x7F))<<shift);
            shift += 7;
        } while ((w&0x80) > 0);
        return ww;
    }
    template<class t_bit_istream,typename t_itr>
    static void decode(const t_bit_istream& is,t_itr it,size_t n)
    {
        for (size_t i=0; i<n; i++) {
            *it = decode(is);
            ++it;
        }
    }
};

struct u32 {
    static std::string type() {
        return "u32";
    }

    template<class t_bit_ostream,typename T>
    static void encode_check_size(t_bit_ostream& os,T y)
    {
        static_assert(std::numeric_limits<T>::is_signed == false,"can only encode unsigned integers");
        os.expand_if_needed(32);
        os.put_int_no_size_check(y,32);
    }
    template<class t_bit_ostream,typename T>
    static void encode(t_bit_ostream& os,T y)
    {
        static_assert(std::numeric_limits<T>::is_signed == false,"can only encode unsigned integers");
        os.put_int_no_size_check(y,32);
    }
    template<class t_bit_ostream,typename t_itr>
    static void encode(t_bit_ostream& os,t_itr begin,t_itr end)
    {
        auto num_elems = std::distance(begin,end);
        os.expand_if_needed(32*num_elems);
        auto tmp = begin;
        while (tmp != end) {
            encode(os,*tmp);
            ++tmp;
        }
    }
    template<class t_bit_istream>
    static uint64_t decode(const t_bit_istream& is)
    {
        uint32_t w32= is.get_int(32);
        return w32;
    }
    template<class t_bit_istream,typename t_itr>
    static void decode(const t_bit_istream& is,t_itr it,size_t n)
    {
        for (size_t i=0; i<n; i++) {
            *it = decode(is);
            ++it;
        }
    }
};

template<class t_coder>
struct delta {
    static std::string type() {
        return "delta+"+t_coder::type();
    }

    template<typename t_itr>
    static inline uint64_t encoded_length(t_itr begin,t_itr end)
    {
        uint64_t bits_required = t_coder::encoded_length(*begin);
        auto prev = begin;
        auto cur = prev+1;
        while (cur != end) {
            bits_required += t_coder::encoded_length(*cur-*prev);
            prev = cur;
            ++cur;
        }
        return bits_required;
    }
    template<class t_bit_ostream,typename t_itr>
    static void encode(t_bit_ostream& os,t_itr begin,t_itr end)
    {
        uint64_t bits_required = encoded_length(begin,end);
        os.expand_if_needed(bits_required);
        auto prev = begin;
        auto cur = prev+1;
        t_coder::encode(os,*begin);
        while (cur != end) {
            t_coder::encode(os,*cur-*prev);
            prev = cur;
            ++cur;
        }
    }
    template<class t_bit_istream,typename t_itr>
    static void decode(const t_bit_istream& is,t_itr it,size_t n)
    {
        auto prev = t_coder::decode(is);
        *it++ = prev;
        for (size_t i=1; i<n; i++) {
            *it = prev + t_coder::decode(is);
            prev = *it;
            ++it;
        }
    }
};

template<uint8_t t_level = 6>
struct zlib {
    static const uint32_t zlib_buf_len = 100000;
    static std::string type() {
        return "zlib";
    }

    template<class t_bit_ostream,typename t_itr>
    static void encode(t_bit_ostream& os,t_itr begin,t_itr end)
    {
        static thread_local uint32_t buf[zlib_buf_len];
        auto n = std::distance(begin,end);
        if(n > zlib_buf_len) {
            LOG(FATAL) << "zlib-encode: zlib_buf_len < n!";
        }
        std::copy(begin,end,std::begin(buf));

        uint64_t bits_required = 32+n*128; // upper bound
        os.expand_if_needed(bits_required);
        os.align8(); // align to bytes if needed
        
        /* space for writing the encoding size */
        uint32_t* out_size = (uint32_t*) os.cur_data();
        os.skip(32);

        /* encode */
        uint64_t written_bytes = zlib_buf_len*sizeof(uint32_t);
        auto out_buf = os.cur_data8();
        uint8_t* in_buf = (uint8_t*)buf;
        uint64_t in_size = n*sizeof(uint32_t);
        auto error = compress2(out_buf,&written_bytes,in_buf,in_size,t_level);
        if(error != Z_OK) {
            switch(error) {
                case Z_MEM_ERROR:
                    LOG(FATAL) << "zlib-encode: Memory error!";
                    break;

                case Z_BUF_ERROR:
                    LOG(FATAL) << "zlib-encode: Buffer error!";
                    break;
                default:
                    LOG(FATAL) << "zlib-encode: Unknown error!";
                    break;
            }
        }
        // write the len. assume it fits in 32bits
        *out_size = (uint32_t) written_bytes;
        os.skip(*out_size*8); // skip over the written content
    }
    template<class t_bit_istream,typename t_itr>
    static void decode(const t_bit_istream& is,t_itr it,size_t n)
    {
        thread_local uint32_t buf[zlib_buf_len];
        if(n > zlib_buf_len) {
            LOG(FATAL) << "zlib-decode: zlib_buf_len < n!";
        }
        is.align8(); // align to bytes if needed

        /* read the encoding size */
        uint32_t* pin_size = (uint32_t*) is.cur_data();
        uint32_t in_size = *pin_size;
        is.skip(32);

        /* decode */
        auto in_buf = is.cur_data8();
        uint8_t* out_buf = (uint8_t*)buf;
        uint64_t out_size = zlib_buf_len*sizeof(uint32_t);
        auto error = uncompress(out_buf,&out_size,in_buf,in_size);
        if(error != Z_OK) {
            switch(error) {
                case Z_MEM_ERROR:
                    LOG(FATAL) << "zlib-decode: Memory error!";
                    break;
                case Z_BUF_ERROR:
                    LOG(FATAL) << "zlib-decode: Buffer error!";
                    break;
                default:
                    LOG(FATAL) << "zlib-decode: Unknown error!";
                    break;
            }
        }
        is.seek(in_size*8); // skip over the read content

        /* output the data from the buffer */
        for(size_t i=0;i<n;i++) {
            *it = buf[i];
            ++it;
        }
    }
};

}
