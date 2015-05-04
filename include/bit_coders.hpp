#pragma once

#include "sdsl/int_vector.hpp"
#include "bit_streams.hpp"
#include "zlib.h"
#include "lzma.h"

#include "logging.hpp"

#include <cassert>
namespace coder {

struct elias_gamma {
    static std::string type()
    {
        return "elias_gamma";
    }

    template <typename T>
    inline uint64_t encoded_length(const T& x) const
    {
        uint8_t len_1 = sdsl::bits::hi(x);
        return len_1 * 2 + 1;
    }
    template <class t_bit_ostream, typename T>
    inline void encode_check_size(t_bit_ostream& os, const T& x) const
    {
        static_assert(std::numeric_limits<T>::is_signed == false, "can only encode unsigned integers");
        uint8_t len_1 = sdsl::bits::hi(x);
        os.expand_if_needed(len_1 * 2 + 1);
        os.put_unary_no_size_check(len_1);
        if (len_1) {
            os.put_int_no_size_check(x, len_1);
        }
    }
    template <class t_bit_ostream, typename T>
    inline void encode(t_bit_ostream& os, const T& x) const
    {
        static_assert(std::numeric_limits<T>::is_signed == false, "can only encode unsigned integers");
        uint8_t len_1 = sdsl::bits::hi(x);
        os.put_unary_no_size_check(len_1);
        if (len_1) {
            os.put_int_no_size_check(x, len_1);
        }
    }
    template <class t_bit_ostream, typename t_itr>
    inline void encode(t_bit_ostream& os, t_itr begin, t_itr end) const
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
            encode(os, *tmp);
            ++tmp;
        }
    }
    template <class t_bit_istream>
    inline uint64_t decode(const t_bit_istream& is) const
    {
        uint64_t x;
        auto len = is.get_unary();
        x = 1ULL << len;
        if (len)
            x |= is.get_int(len);
        return x;
    }
    template <class t_bit_istream, typename t_itr>
    inline void decode(const t_bit_istream& is, t_itr it, size_t n) const
    {
        for (size_t i = 0; i < n; i++) {
            *it = decode(is);
            ++it;
        }
    }
};

struct elias_delta {
    static std::string type()
    {
        return "elias_delta";
    }

    template <typename T>
    inline uint64_t encoded_length(const T& x) const
    {
        uint8_t len_1 = sdsl::bits::hi(x);
        return len_1 + (sdsl::bits::hi(len_1 + 1) << 1) + 1;
    }
    template <class t_bit_ostream, typename T>
    inline void encode_check_size(t_bit_ostream& os, const T& x) const
    {
        static_assert(std::numeric_limits<T>::is_signed == false, "can only encode unsigned integers");
        // (number of sdsl::bits to represent x)
        uint8_t len = sdsl::bits::hi(x) + 1;
        // (number of sdsl::bits to represent the length of x) -1
        uint8_t len_1_len = sdsl::bits::hi(len);
        uint8_t bits = len + (len_1_len << 1) + 1;
        os.expand_if_needed(bits);

        // Write unary representation for the length of the length of x
        os.put_unary_no_size_check(len_1_len);
        if (len_1_len) {
            os.put_int_no_size_check(len, len_1_len);
            os.put_int_no_size_check(x, len - 1);
        }
    }
    template <class t_bit_ostream, typename T>
    inline void encode(t_bit_ostream& os, const T& x) const
    {
        static_assert(std::numeric_limits<T>::is_signed == false, "can only encode unsigned integers");
        // (number of sdsl::bits to represent x)
        uint8_t len = sdsl::bits::hi(x) + 1;
        // (number of sdsl::bits to represent the length of x) -1
        uint8_t len_1_len = sdsl::bits::hi(len);
        // Write unary representation for the length of the length of x
        os.put_unary_no_size_check(len_1_len);
        if (len_1_len) {
            os.put_int_no_size_check(len, len_1_len);
            os.put_int_no_size_check(x, len - 1);
        }
    }
    template <class t_bit_ostream, typename t_itr>
    inline void encode(t_bit_ostream& os, t_itr begin, t_itr end) const
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
            encode(os, *tmp);
            ++tmp;
        }
    }
    template <class t_bit_istream>
    inline uint64_t decode(const t_bit_istream& is) const
    {
        uint64_t x = 1;
        auto len_1_len = is.get_unary();
        if (len_1_len) {
            auto len_1 = is.get_int(len_1_len);
            auto len = len_1 + (1ULL << len_1_len);
            x = is.get_int(len - 1);
            x = x + (len - 1 < 64) * (1ULL << (len - 1));
        }
        return x;
    }
    template <class t_bit_istream, typename t_itr>
    inline void decode(const t_bit_istream& is, t_itr it, size_t n) const
    {
        for (size_t i = 0; i < n; i++) {
            *it = decode(is);
            ++it;
        }
    }
};

struct vbyte {
    static std::string type()
    {
        return "vbyte";
    }

    template <typename T>
    inline uint64_t encoded_length(const T& x) const
    {
        const uint8_t vbyte_len[64] = { 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4,
                                        5, 5, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8 };
        return 8 * vbyte_len[sdsl::bits::hi(x) + 1];
    }
    template <class t_bit_ostream, typename T>
    inline void encode_check_size(t_bit_ostream& os, T y) const
    {
        static_assert(std::numeric_limits<T>::is_signed == false, "can only encode unsigned integers");
        os.expand_if_needed(encoded_length(y));
        uint64_t x = y;
        uint8_t w = x & 0x7F;
        x >>= 7;
        while (x > 0) {
            w |= 0x80; // mark overflow bit
            os.put_int_no_size_check(w, 8);
            w = x & 0x7F;
            x >>= 7;
        }
        os.put_int_no_size_check(w, 8);
    }
    template <class t_bit_ostream, typename T>
    inline void encode(t_bit_ostream& os, T y) const
    {
        static_assert(std::numeric_limits<T>::is_signed == false, "can only encode unsigned integers");
        uint64_t x = y;
        uint8_t w = x & 0x7F;
        x >>= 7;
        while (x > 0) {
            w |= 0x80; // mark overflow bit
            os.put_int_no_size_check(w, 8);
            w = x & 0x7F;
            x >>= 7;
        }
        os.put_int_no_size_check(w, 8);
    }
    template <class t_bit_ostream, typename t_itr>
    inline void encode(t_bit_ostream& os, t_itr begin, t_itr end) const
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
            encode(os, *tmp);
            ++tmp;
        }
    }
    template <class t_bit_istream>
    inline uint64_t decode(const t_bit_istream& is) const
    {
        uint64_t ww = 0;
        uint8_t w = 0;
        uint64_t shift = 0;
        do {
            w = is.get_int(8);
            ww |= (((uint64_t)(w & 0x7F)) << shift);
            shift += 7;
        } while ((w & 0x80) > 0);
        return ww;
    }
    template <class t_bit_istream, typename t_itr>
    inline void decode(const t_bit_istream& is, t_itr it, size_t n) const
    {
        for (size_t i = 0; i < n; i++) {
            *it = decode(is);
            ++it;
        }
    }
};

struct u32 {
    static std::string type()
    {
        return "u32";
    }

    template <class t_bit_ostream, typename T>
    inline void encode_check_size(t_bit_ostream& os, T y) const
    {
        static_assert(std::numeric_limits<T>::is_signed == false, "can only encode unsigned integers");
        os.expand_if_needed(32);
        os.put_int_no_size_check(y, 32);
    }
    template <class t_bit_ostream, typename T>
    inline void encode(t_bit_ostream& os, T y) const
    {
        static_assert(std::numeric_limits<T>::is_signed == false, "can only encode unsigned integers");
        os.put_int_no_size_check(y, 32);
    }
    template <class t_bit_ostream, typename t_itr>
    inline void encode(t_bit_ostream& os, t_itr begin, t_itr end) const
    {
        auto num_elems = std::distance(begin, end);
        os.expand_if_needed(32 * num_elems);
        auto tmp = begin;
        while (tmp != end) {
            encode(os, *tmp);
            ++tmp;
        }
    }
    template <class t_bit_istream>
    inline uint64_t decode(const t_bit_istream& is) const
    {
        uint32_t w32 = is.get_int(32);
        return w32;
    }
    template <class t_bit_istream, typename t_itr>
    inline void decode(const t_bit_istream& is, t_itr it, size_t n) const
    {
        for (size_t i = 0; i < n; i++) {
            *it = decode(is);
            ++it;
        }
    }
};

struct u32a {
public:
    static const uint32_t u32a_buf_len = 100000;
private:    
    mutable uint32_t buf[u32a_buf_len];
public:
    static std::string type()
    {
        return "u32a";
    }

    template <class t_bit_ostream, typename t_itr>
    inline void encode(t_bit_ostream& os, t_itr begin, t_itr end) const
    {
        std::copy(begin,end,std::begin(buf));
        auto num_elems = std::distance(begin, end);
        os.expand_if_needed(8+ 32 * num_elems);
        os.align8();
        uint8_t* out = os.cur_data8();
        uint32_t* out32 = (uint32_t*)out;
        std::copy(begin,end,out32);
        os.skip(32*num_elems);
    }
    template <class t_bit_istream, typename t_itr>
    inline void decode(const t_bit_istream& is, t_itr it, size_t n) const
    {
        is.align8();
        const uint8_t* in = is.cur_data8();
        const uint32_t* is32 = (uint32_t*)in;
        std::copy(is32,is32+n,it);
        is.skip(32*n);
    }
};

template <uint8_t t_level = 6>
struct zlib {
public:
    static const uint32_t zlib_buf_len = 100000;
    static const uint32_t mem_level = 9;
    static const uint32_t window_bits = 15;

private:
    mutable uint32_t buf[zlib_buf_len];
    mutable z_stream dstrm;
    mutable z_stream istrm;

public:
    zlib()
    {
        dstrm.zalloc = Z_NULL;
        dstrm.zfree = Z_NULL;
        dstrm.opaque = Z_NULL;
        dstrm.next_in = (uint8_t*)buf;
        deflateInit2(&dstrm,
                     t_level,
                     Z_DEFLATED,
                     window_bits,
                     mem_level,
                     Z_DEFAULT_STRATEGY);
        istrm.zalloc = Z_NULL;
        istrm.zfree = Z_NULL;
        istrm.opaque = Z_NULL;
        istrm.next_in = (uint8_t*)buf;
        inflateInit2(&istrm, window_bits);
    }
    ~zlib()
    {
        deflateEnd(&dstrm);
        inflateEnd(&istrm);
    }

public:
    static std::string type()
    {
        return "zlib-" + std::to_string(t_level);
    }

    template <class t_bit_ostream, typename t_itr>
    inline void encode(t_bit_ostream& os, t_itr begin, t_itr end) const
    {
        auto n = std::distance(begin, end);
        if (n > zlib_buf_len) {
            LOG(FATAL) << "zlib-encode: zlib_buf_len < n";
        }
        std::copy(begin, end, std::begin(buf));

        uint64_t bits_required = 32 + n * 128; // upper bound
        os.expand_if_needed(bits_required);
        os.align8(); // align to bytes if needed

        /* space for writing the encoding size */
        uint32_t* out_size = (uint32_t*)os.cur_data8();
        os.skip(32);

        /* encode */
        uint8_t* out_buf = os.cur_data8();
        uint64_t in_size = n * sizeof(uint32_t);

        uint32_t out_buf_bytes = bits_required >> 3;

        dstrm.avail_in = in_size;
        dstrm.avail_out = out_buf_bytes;
        dstrm.next_in = (uint8_t*)buf;
        dstrm.next_out = out_buf;

        auto error = deflate(&dstrm, Z_FINISH);
        deflateReset(&dstrm); // after finish we have to reset

        /* If the parameter flush is set to Z_FINISH, pending input
         is processed, pending output is flushed and deflate returns
         with Z_STREAM_END if there was enough output space; if 
         deflate returns with Z_OK, this function must be called
         again with Z_FINISH and more output spac */
        if (error != Z_STREAM_END) {
            switch (error) {
            case Z_MEM_ERROR:
                LOG(FATAL) << "zlib-encode: Memory error!";
                break;
            case Z_BUF_ERROR:
                LOG(FATAL) << "zlib-encode: Buffer error!";
                break;
            case Z_OK:
                LOG(FATAL) << "zlib-encode: need to call deflate again!";
                break;
            case Z_DATA_ERROR:
                LOG(FATAL) << "zlib-encode: Invalid or incomplete deflate data!";
                break;
            default:
                LOG(FATAL) << "zlib-encode: Unknown error: " << error;
                break;
            }
        }
        if (dstrm.avail_in != 0) {
            LOG(FATAL) << "zlib-encode: not everything was encoded!";
        }
        // write the len. assume it fits in 32bits
        uint32_t written_bytes = out_buf_bytes - dstrm.avail_out;
        *out_size = (uint32_t)written_bytes;
        os.skip(written_bytes * 8); // skip over the written content
    }
    template <class t_bit_istream, typename t_itr>
    inline void decode(const t_bit_istream& is, t_itr it, size_t n) const
    {
        if (n > zlib_buf_len) {
            LOG(FATAL) << "zlib-decode: zlib_buf_len < n";
        }
        is.align8(); // align to bytes if needed

        /* read the encoding size */
        uint32_t* pin_size = (uint32_t*)is.cur_data8();
        uint32_t in_size = *pin_size;
        is.skip(32);

        /* decode */
        auto in_buf = is.cur_data8();
        uint8_t* out_buf = (uint8_t*)buf;
        uint64_t out_size = zlib_buf_len * sizeof(uint32_t);

        istrm.avail_in = in_size;
        istrm.next_in = (uint8_t*)in_buf;
        istrm.avail_out = out_size;
        istrm.next_out = out_buf;
        auto error = inflate(&istrm, Z_FINISH);
        inflateReset(&istrm); // after finish we need to reset
        if (error != Z_STREAM_END) {
            switch (error) {
            case Z_MEM_ERROR:
                LOG(FATAL) << "zlib-decode: Memory error!";
                break;
            case Z_BUF_ERROR:
                LOG(FATAL) << "zlib-decode: Buffer error!";
                break;
            case Z_DATA_ERROR:
                LOG(FATAL) << "zlib-decode: Data error!";
                break;
            case Z_STREAM_END:
                LOG(FATAL) << "zlib-decode: Stream end error!";
                break;
            default:
                LOG(FATAL) << "zlib-decode: Unknown error: " << error;
                break;
            }
        }

        is.skip(in_size * 8); // skip over the read content

        /* output the data from the buffer */
        for (size_t i = 0; i < n; i++) {
            *it = buf[i];
            ++it;
        }
    }
};

template <uint8_t t_level = 3>
struct lzma {
public:
    static const uint32_t lzma_buf_len = 100000;
    static const uint32_t lzma_mem_limit = 128 * 1024 * 1024;
    static const uint32_t lzma_max_mem_limit = 1024 * 1024 * 1024;

private:
    mutable uint32_t buf[lzma_buf_len];
    mutable lzma_stream strm;

public:
    lzma()
    {
        strm = LZMA_STREAM_INIT;
    }
    ~lzma()
    {
        lzma_end(&strm);
    }

public:
    static std::string type()
    {
        return "lzma-" + std::to_string(t_level);
    }

    template <class t_bit_ostream, typename t_itr>
    inline void encode(t_bit_ostream& os, t_itr begin, t_itr end) const
    {
        auto n = std::distance(begin, end);
        if (n > lzma_buf_len) {
            LOG(FATAL) << "lzma-encode: lzma_buf_len < n!";
        }
        std::copy(begin, end, std::begin(buf));

        uint64_t bits_required = 32 + n * 128; // upper bound
        os.expand_if_needed(bits_required);
        os.align8(); // align to bytes if needed

        /* space for writing the encoding size */
        uint32_t* out_size = (uint32_t*)os.cur_data8();
        os.skip(32);

        /* init compressor */
        uint8_t* out_buf = os.cur_data8();
        uint8_t* in_buf = (uint8_t*)buf;
        uint64_t in_size = n * sizeof(uint32_t);
        strm.next_in = in_buf;
        strm.avail_in = in_size;
        strm.next_out = out_buf;
        strm.avail_out = bits_required >> 3;

        /* compress */
        int res;
        if ((res = lzma_easy_encoder(&strm, t_level, LZMA_CHECK_NONE)) != LZMA_OK) {
            LOG(FATAL) << "lzma-encode: error init LMZA encoder < n!";
        }
        if ((res = lzma_code(&strm, LZMA_RUN)) != LZMA_OK) {
            LOG(FATAL) << "lzma-encode: error init LZMA_RUN < n!";
        }

        if ((res = lzma_code(&strm, LZMA_FINISH)) != LZMA_STREAM_END && res != LZMA_OK) {
            LOG(FATAL) << "lzma-encode: error init LZMA_FINISH < n!";
        }

        *out_size = (uint32_t)strm.total_out;
        os.skip(strm.total_out * 8); // skip over the written content
    }

    template <class t_bit_istream, typename t_itr>
    inline void decode(const t_bit_istream& is, t_itr it, size_t n) const
    {
        if (n > lzma_buf_len) {
            LOG(FATAL) << "lzma-decode: lzma_buf_len < n!";
        }
        is.align8(); // align to bytes if needed

        /* read the encoding size */
        uint32_t* pin_size = (uint32_t*)is.cur_data8();
        uint32_t in_size = *pin_size;
        is.skip(32);

        /* setup decoder */
        auto in_buf = is.cur_data8();
        uint8_t* out_buf = (uint8_t*)buf;
        uint64_t out_size = lzma_buf_len * sizeof(uint32_t);

        strm.next_in = in_buf;
        strm.avail_in = in_size;
        strm.next_out = out_buf;
        strm.avail_out = out_size;

        int res;
        uint32_t cur_mem_limit = lzma_mem_limit;
        if ((res = lzma_auto_decoder(&strm, cur_mem_limit, 0)) != LZMA_OK) {
            lzma_end(&strm);
            LOG(FATAL) << "lzma-encode: error init LMZA decoder";
        }

        /* decode */
        do {
            if ((res = lzma_code(&strm, LZMA_RUN)) != LZMA_STREAM_END) {
                if (res == LZMA_MEMLIMIT_ERROR) {
                    cur_mem_limit *= 2; // double mem limit
                } else {
                    lzma_end(&strm);
                    LOG(FATAL) << "lzma-encode: error decoding LZMA_RUN";
                }
            }
        } while ((res == LZMA_MEMLIMIT_ERROR) && (cur_mem_limit < lzma_max_mem_limit));

        /* finish decoding */
        lzma_end(&strm);
        is.skip(in_size * 8); // skip over the read content

        /* output the data from the buffer */
        for (size_t i = 0; i < n; i++) {
            *it = buf[i];
            ++it;
        }
    }
};

//with the dict_index_sa_length_selector strategy - match lengths are a known multiple of LM. This coder simply removes/adds the LM component, before
// passing on to the next stage of coding say U32 or Zlib (given by t_coder c)
template <int lm, class t_coder>
struct length_multiplier {
public:
    static const uint32_t lm_buf_len = 100000;

private:
    mutable uint32_t buf[lm_buf_len];
    t_coder next_stage_coder;
public:
    static std::string type()
    {
        return "length_multiplier-" + std::to_string(lm) + "-" + t_coder::type();
    }

    template <class t_bit_ostream, typename T>
    void encode_check_size(t_bit_ostream& os, T y) const
    {
        assert((y % lm) == 0); //"invalid number for length_multiplier"
        next_stage_coder.encode_check_size(os, y / lm);
    }
    template <class t_bit_ostream, typename T>
    void encode(t_bit_ostream& os, T y) const
    {
        assert((y % lm) == 0); //"invalid number for length_multiplier"
        next_stage_coder.encode(os, y / lm);
    }
    template <class t_bit_ostream, typename t_itr>
    void encode(t_bit_ostream& os, t_itr begin, t_itr end) const
    {
        size_t num_elems = std::distance(begin,end);
        auto buf_itr = std::begin(buf);
        for(size_t i=0;i<num_elems;i++) {
            *buf_itr = *begin / lm;
            ++buf_itr;
            ++begin;
        }
        next_stage_coder.encode(os,std::begin(buf),buf_itr);
    }
    template <class t_bit_istream>
    uint64_t decode(const t_bit_istream& is) const
    {
        return lm * next_stage_coder.decode(is);
    }
    template <class t_bit_istream, typename t_itr>
    void decode(const t_bit_istream& is, t_itr it, size_t n) const
    {
        auto itr = it;
        next_stage_coder.decode(is,it,n);
        for (size_t i = 0; i < n; i++) {
            auto elem = *itr;
            *itr = lm * elem;
            ++itr;
        }
    }
};


}
