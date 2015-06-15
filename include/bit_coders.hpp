#pragma once

#include "sdsl/int_vector.hpp"
#include "bit_streams.hpp"
#include "zlib.h"
#include "lzma.h"
#include "lz4hc.h"
#include "lz4.h"

#include "logging.hpp"

#include <cassert>
namespace coder {

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
    template <class t_bit_ostream, class T>
    inline void encode(t_bit_ostream& os, T* in_buf, size_t n) const
    {
        uint64_t bits_required = 0;
        auto tmp = in_buf;
        for (size_t i = 0; i < n; i++) {
            bits_required += encoded_length(*tmp);
            ++tmp;
        }
        os.expand_if_needed(bits_required);
        tmp = in_buf;
        for (size_t i = 0; i < n; i++) {
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
    template <class t_bit_istream, class T>
    inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
    {
        for (size_t i = 0; i < n; i++) {
            *out_buf = decode(is);
            ++out_buf;
        }
    }
};

template <uint8_t t_width>
struct fixed {
public:
    static std::string type()
    {
        return "u" + std::to_string(t_width);
    }

    template <class t_bit_ostream, class T>
    inline void encode(t_bit_ostream& os, T* in_buf, size_t n) const
    {
        os.expand_if_needed(t_width * n);
        for (size_t i = 0; i < n; i++)
            os.put_int_no_size_check(in_buf[i], t_width);
    }
    template <class t_bit_istream, class T>
    inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
    {
        for (size_t i = 0; i < n; i++)
            out_buf[i] = is.get_int(t_width);
    }
};

template <class t_int_type>
struct aligned_fixed {
public:
    static std::string type()
    {
        return "u" + std::to_string(8 * sizeof(t_int_type)) + "a";
    }

    template <class t_bit_ostream>
    inline void encode(t_bit_ostream& os, const t_int_type* in_buf, size_t n) const
    {
        os.expand_if_needed(8 + 8*sizeof(t_int_type) * n);
        os.align8();
        uint8_t* out = os.cur_data8();
        t_int_type* outA = (t_int_type*)out;
        std::copy(in_buf, in_buf + n, outA);
        os.skip(sizeof(t_int_type) * 8 * n);
    }
    template <class t_bit_istream>
    inline void decode(const t_bit_istream& is, t_int_type* out_buf, size_t n) const
    {
        is.align8();
        const uint8_t* in = is.cur_data8();
        const t_int_type* isA = (t_int_type*)in;
        std::copy(isA, isA + n, out_buf);
        is.skip(sizeof(t_int_type) * 8 * n);
    }
};

template <uint8_t t_level = 6>
struct zlib {
public:
    static const uint32_t mem_level = 9;
    static const uint32_t window_bits = 15;

private:
    mutable z_stream dstrm;
    mutable z_stream istrm;

public:
    zlib()
    {
        dstrm.zalloc = Z_NULL;
        dstrm.zfree = Z_NULL;
        dstrm.opaque = Z_NULL;
        deflateInit2(&dstrm,
                     t_level,
                     Z_DEFLATED,
                     window_bits,
                     mem_level,
                     Z_DEFAULT_STRATEGY);
        istrm.zalloc = Z_NULL;
        istrm.zfree = Z_NULL;
        istrm.opaque = Z_NULL;
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

    template <class t_bit_ostream, class T>
    inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
    {
        uint64_t bits_required = 32 + n * 128; // upper bound
        os.expand_if_needed(bits_required);
        os.align8(); // align to bytes if needed

        /* space for writing the encoding size */
        uint32_t* out_size = (uint32_t*)os.cur_data8();
        os.skip(32);

        /* encode */
        uint8_t* out_buf = os.cur_data8();
        uint64_t in_size = n * sizeof(T);

        uint32_t out_buf_bytes = bits_required >> 3;

        dstrm.avail_in = in_size;
        dstrm.avail_out = out_buf_bytes;
        dstrm.next_in = (uint8_t*)in_buf;
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
    template <class t_bit_istream, class T>
    inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
    {
        is.align8(); // align to bytes if needed

        /* read the encoding size */
        uint32_t* pin_size = (uint32_t*)is.cur_data8();
        uint32_t in_size = *pin_size;
        is.skip(32);

        /* decode */
        auto in_buf = is.cur_data8();
        uint64_t out_size = n * sizeof(T);

        istrm.avail_in = in_size;
        istrm.next_in = (uint8_t*)in_buf;
        istrm.avail_out = out_size;
        istrm.next_out = (uint8_t*)out_buf;
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
    }
};

template <uint8_t t_level = 3>
struct lzma {
public:
    static const uint32_t lzma_mem_limit = 128 * 1024 * 1024;
    static const uint32_t lzma_max_mem_limit = 1024 * 1024 * 1024;

private:
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

    template <class t_bit_ostream, class T>
    inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
    {
        uint64_t bits_required = 32 + n * 128; // upper bound
        os.expand_if_needed(bits_required);
        os.align8(); // align to bytes if needed

        /* space for writing the encoding size */
        uint32_t* out_size = (uint32_t*)os.cur_data8();
        os.skip(32);

        /* init compressor */
        uint8_t* out_buf = os.cur_data8();
        uint64_t in_size = n * sizeof(T);
        strm.next_in = (uint8_t*)in_buf;
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

    template <class t_bit_istream, class T>
    inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
    {
        is.align8(); // align to bytes if needed

        /* read the encoding size */
        uint32_t* pin_size = (uint32_t*)is.cur_data8();
        uint32_t in_size = *pin_size;
        is.skip(32);

        /* setup decoder */
        auto in_buf = is.cur_data8();
        uint64_t out_size = n * sizeof(T);

        strm.next_in = in_buf;
        strm.avail_in = in_size;
        strm.next_out = (uint8_t*)out_buf;
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
    }
};


template <uint8_t t_level = 9>
struct lz4hc {
private:
    mutable void* lz4_state = nullptr;
public:
    lz4hc()
    {
        auto state_size = LZ4_sizeofStateHC();
        lz4_state = malloc(state_size);
    }
    ~lz4hc()
    {
        free(lz4_state);
    }

public:
    static std::string type()
    {
        return "lz4hc-" + std::to_string(t_level);
    }

    template <class t_bit_ostream, class T>
    inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
    {
        uint64_t bits_required = 32 + n * 128; // upper bound
        os.expand_if_needed(bits_required);
        os.align8(); // align to bytes if needed
        /* compress */
        char* out_buf = (char*) os.cur_data8();
        uint64_t in_size = n * sizeof(T);
        auto bytes_written = LZ4_compress_HC_extStateHC(lz4_state,(const char*)in_buf,out_buf,in_size,bits_required >> 3,t_level);
        os.skip(bytes_written * 8); // skip over the written content
    }

    template <class t_bit_istream, class T>
    inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
    {
        is.align8(); // align to bytes if needed
        const char* in_buf = (const char*) is.cur_data8();
        uint64_t out_size = n * sizeof(T);
        auto comp_size = LZ4_decompress_fast(in_buf,(char*)out_buf,out_size);
        is.skip(comp_size * 8); // skip over the read content
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
    template <class t_bit_ostream, class T>
    inline void encode(t_bit_ostream& os, T* in_buf, size_t n) const
    {
        for (size_t i = 0; i < n; i++) {
            buf[i] = in_buf[i] / lm;
        }
        next_stage_coder.encode(os, buf, n);
    }
    template <class t_bit_istream, class T>
    inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
    {
        next_stage_coder.decode(is, out_buf, n);
        for (size_t i = 0; i < n; i++) {
            out_buf[i] = out_buf[i] * lm;
        }
    }
};
}
