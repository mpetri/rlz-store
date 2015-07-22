#pragma once

#include "sdsl/int_vector.hpp"
#include "bit_streams.hpp"
#include "zlib.h"
#include "lzma.h"
#include "lz4hc.h"
#include "lz4.h"
#include "bzlib.h"

#include "logging.hpp"

#include <cassert>
namespace coder {

struct vbyte {
    static std::string type()
    {
        return "vbyte";
    }

    inline void set_deflate_dictionary(const uint8_t* , uint32_t ) const
    {

    }

    inline void set_inflate_dictionary(const uint8_t* , uint32_t ) const
    {
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

    inline void set_deflate_dictionary(const uint8_t* , uint32_t ) const
    {

    }

    inline void set_inflate_dictionary(const uint8_t* , uint32_t ) const
    {
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

    inline void set_deflate_dictionary(const uint8_t* , uint32_t ) const
    {

    }

    inline void set_inflate_dictionary(const uint8_t* , uint32_t ) const
    {
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
    mutable const uint8_t* dict_ptr = nullptr;
    mutable uint32_t dict_size = 0;
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

    inline void set_deflate_dictionary(const uint8_t* dict_ptr, uint32_t n) const
    {
        auto ret = deflateSetDictionary(&dstrm,dict_ptr,n);
        if (ret != Z_OK) {
            LOG(FATAL) << "zlib-encode: set dictionary error:" << ret;
        }
    }

    inline void set_inflate_dictionary(const uint8_t* dptr, uint32_t n) const
    {
        dict_ptr = dptr;
        dict_size = n;
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
        if(error == Z_NEED_DICT) {
            auto sdret = inflateSetDictionary(&istrm,dict_ptr,dict_size);
            if (sdret != Z_OK) {
                LOG(FATAL) << "zlib-decode: set dictionary error:" << sdret;
            }
            error = inflate(&istrm, Z_FINISH);
        }
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
            case Z_NEED_DICT:
                LOG(FATAL) << "zlib-decode: NEED DICT!";
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
    mutable lzma_stream strm_enc;
    mutable lzma_stream strm_dec;
public:
    lzma()
    {
        strm_enc = LZMA_STREAM_INIT;
        strm_dec = LZMA_STREAM_INIT;
    }
    ~lzma()
    {
        lzma_end(&strm_enc);
        lzma_end(&strm_dec);
    }

public:
    static std::string type()
    {
        return "lzma-" + std::to_string(t_level);
    }

    template <class t_bit_ostream, class T>
    inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
    {
        uint64_t bits_required = 2048 + n * 256; // upper bound
        os.expand_if_needed(bits_required);
        os.align8(); // align to bytes if needed

        /* space for writing the encoding size */
        uint32_t* out_size = (uint32_t*)os.cur_data8();
        os.skip(32);

        /* init compressor */
        uint32_t osize = bits_required >> 3;
        uint8_t* out_buf = os.cur_data8();
        uint64_t in_size = n * sizeof(T);
        strm_enc.next_in = (uint8_t*)in_buf;
        strm_enc.avail_in = in_size;
        strm_enc.total_out = 0;
        strm_enc.total_in = 0;

        /* compress */
        int res;
        if ((res = lzma_easy_encoder(&strm_enc, t_level, LZMA_CHECK_NONE)) != LZMA_OK) {
            LOG(FATAL) << "lzma-encode: error init LMZA encoder < n!";
        }

        auto total_written_bytes = 0ULL;
        while(res != LZMA_STREAM_END) {
            strm_enc.next_out = out_buf;
            strm_enc.avail_out = osize;
            if ((res = lzma_code(&strm_enc, LZMA_FINISH)) != LZMA_OK && res != LZMA_STREAM_END ) {
                LOG(FATAL) << "lzma-encode: error code LZMA_FINISH < n!: " << res;
            }
            auto written_bytes = osize - strm_enc.avail_out;
            total_written_bytes += written_bytes;
            osize -= written_bytes;
            out_buf += written_bytes;
        }

        *out_size = (uint32_t)total_written_bytes;
        os.skip(total_written_bytes * 8); // skip over the written content
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
        int res;
        if ((res = lzma_auto_decoder(&strm_dec, lzma_mem_limit, 0)) != LZMA_OK) {
            LOG(FATAL) << "lzma-decode: error init LMZA decoder:" << res;
        }
        
        strm_dec.next_in = in_buf;
        strm_dec.avail_in = in_size;
        auto total_decoded = 0ULL;
        while(res != LZMA_STREAM_END) {
            strm_dec.next_out = (uint8_t*)out_buf;
            strm_dec.avail_out = out_size;
            /* decode */
            res = lzma_code (&strm_dec, LZMA_RUN);
            auto decoded = out_size - strm_dec.avail_out;
            total_decoded += decoded;
            out_buf += decoded;
            out_size -= decoded;
            if(res != LZMA_OK && res != LZMA_STREAM_END) {
                lzma_end(&strm_dec);
                LOG(FATAL) << "lzma-decode: error decoding LZMA_RUN: " << res;
            }
            if(res == LZMA_STREAM_END) break;
        }
        
        if(total_decoded != n * sizeof(T)) {
            LOG(ERROR) << "lzma-decode: decoded bytes = " << total_decoded << " should be = " << n * sizeof(T);
        }

        /* finish decoding */
        // lzma_end(&strm);
        is.skip(in_size * 8); // skip over the read content
    }
};


template <uint8_t t_level = 9>
struct lz4hc {
private:
    mutable void* lz4_state = nullptr;
    mutable const char* dict_ptr = nullptr;
    mutable uint32_t dict_size = 0;
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

    inline void set_deflate_dictionary(const uint8_t* dptr, uint32_t n) const
    {
        dict_ptr = (const char*) dptr;
        dict_size = n;
    }

    inline void set_inflate_dictionary(const uint8_t* dptr, uint32_t n) const
    {
        dict_ptr = (const char*) dptr;
        dict_size = n;
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
        LZ4_resetStreamHC((LZ4_streamHC_t*)lz4_state,t_level);
        if(dict_size != 0) { /* prime with dict */
            LZ4_loadDictHC((LZ4_streamHC_t*)lz4_state,dict_ptr,dict_size);
        }
        auto bytes_written = LZ4_compress_HC_continue((LZ4_streamHC_t*)lz4_state,(const char*)in_buf,out_buf,in_size,bits_required >> 3);
        os.skip(bytes_written * 8);
        // } else {
        //     auto bytes_written = LZ4_compress_HC_extStateHC(lz4_state,(const char*)in_buf,out_buf,in_size,bits_required >> 3,t_level);
        //     os.skip(bytes_written * 8); // skip over the written content            
        // }
    }

    template <class t_bit_istream, class T>
    inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
    {
        is.align8(); // align to bytes if needed
        const char* in_buf = (const char*) is.cur_data8();
        uint64_t out_size = n * sizeof(T);
        int comp_size = 0;
        if(dict_size != 0) { /* prime with dict */
            comp_size = LZ4_decompress_fast_usingDict(in_buf,(char*)out_buf,out_size,dict_ptr,dict_size);
        } else {
            comp_size = LZ4_decompress_fast(in_buf,(char*)out_buf,out_size);
        }
        is.skip(comp_size * 8); // skip over the read content
    }
};

template <uint8_t t_level = 6>
struct bzip2 {
public:
    static const int bzip_verbose_level = 0;
    static const int bzip_work_factor = 0;
    static const int bzip_use_small_mem = 0;
public:
    static std::string type()
    {
        return "bzip2-" + std::to_string(t_level);
    }

    inline void set_deflate_dictionary(const uint8_t* , uint32_t ) const
    {

    }

    inline void set_inflate_dictionary(const uint8_t* , uint32_t ) const
    {
    }


    template <class t_bit_ostream, class T>
    inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
    {
        uint64_t bits_required = 32 + n * 512; // upper bound
        os.expand_if_needed(bits_required);
        os.align8(); // align to bytes if needed

        /* space for writing the encoding size */
        uint32_t* out_size = (uint32_t*)os.cur_data8();
        os.skip(32);

        /* encode */
        uint8_t* out_buf = os.cur_data8();
        uint64_t in_size = n * sizeof(T);

        uint32_t written_bytes = bits_required >> 3;
        auto ret = BZ2_bzBuffToBuffCompress((char*)out_buf,&written_bytes,
            (char*)in_buf,in_size,t_level,bzip_verbose_level,bzip_work_factor);

        if(ret != BZ_OK) {
            LOG(FATAL) << "bzip2-encode: encoding error: " << ret;
        }

        // write the len. assume it fits in 32bits
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
        uint32_t out_size = n * sizeof(T);

        auto ret = BZ2_bzBuffToBuffDecompress((char*)out_buf,
            &out_size,(char*)in_buf,in_size,bzip_use_small_mem,bzip_verbose_level);

        if(ret != BZ_OK) {
            LOG(FATAL) << "bzip2-decode: decode error: " << ret;
        }

        if (n * sizeof(T) != out_size) {
            LOG(FATAL) << "bzip2-decode: not everything was decode!";
        }
        is.skip(in_size * 8); // skip over the read content
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
