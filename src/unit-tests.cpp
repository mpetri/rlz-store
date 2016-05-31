#include "gtest/gtest.h"
#include "sdsl/int_vector.hpp"
#include "bit_streams.hpp"
#include "bit_coders.hpp"
#include <functional>
#include <random>

#include "factor_data.hpp"
#include "factor_coder.hpp"
#include "utils.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

TEST(bit_stream, vbyte)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::vbyte c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(bit_stream, fixed)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::fixed<7> c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(bit_stream, aligned_fixed)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::aligned_fixed<uint32_t> c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::aligned_fixed<uint8_t> c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n);
        }
        std::vector<uint8_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(bit_stream, zlib)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::zlib<6> c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(bit_stream, zlib_uint8)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 200);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::zlib<6> c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n);
        }
        std::vector<uint8_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(bit_stream, lz4_uint8)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 200);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::lz4hc<9> c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n);
        }
        std::vector<uint8_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(bit_stream, bzip2)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 200);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::bzip2<9> c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n);
        }
        std::vector<uint8_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}


TEST(bit_stream, elias_fano)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 2000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::elias_fano c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n, 2000);
        }
        auto size_estimate = c.determine_size(A.data(),n,2000);
        ASSERT_TRUE(size_estimate >= bv.size());
        std::vector<uint8_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n, 2000);
            uint64_t pos_after_dec = is.tellg();
            uint64_t bv_size = bv.size();
            ASSERT_EQ(pos_after_dec,bv_size);
        }
        for (size_t j = 0; j < B.size(); j++) {
            ASSERT_EQ(B[j], A[j]);
        }
    }
}

TEST(bit_stream, interpolative_custom)
{
    std::vector<uint8_t> A = {1,4,6,8,9,10,11,12,16,18,19,20};
    coder::interpolative c;
    sdsl::bit_vector bv;
    {
        bit_ostream<sdsl::bit_vector> os(bv);
        c.encode(os, A.data(), A.size(), 20);
    }
    ASSERT_TRUE(bv.size() <= 19);
    std::vector<uint8_t> B(A.size());
    {
        bit_istream<sdsl::bit_vector> is(bv);
        c.decode(is, B.data(), A.size(), 20);
    }
    for (size_t j = 0; j < B.size(); j++) {
        ASSERT_EQ(B[j], A[j]);
    }
}

TEST(bit_stream, interpolative_custom_zero)
{
    std::vector<uint8_t> A = {0,1,4,6,8,9,10,11,12,16,18,19,20};
    coder::interpolative c;
    sdsl::bit_vector bv;
    {
        bit_ostream<sdsl::bit_vector> os(bv);
        c.encode(os, A.data(), A.size(), 20);
    }
    ASSERT_TRUE(bv.size() <= 20);
    std::vector<uint8_t> B(A.size());
    {
        bit_istream<sdsl::bit_vector> is(bv);
        c.decode(is, B.data(), A.size(), 20);
    }
    for (size_t j = 0; j < B.size(); j++) {
        ASSERT_EQ(B[j], A[j]);
    }
}

TEST(bit_stream, interpolative)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 2000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint64_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::interpolative c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n, 2000);
        }
        auto size_estimate = c.determine_size(A.data(),n,2000);
        ASSERT_TRUE(size_estimate >= bv.size());
        std::vector<uint64_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n, 2000);
            uint64_t pos_after_dec = is.tellg();
            uint64_t bv_size = bv.size();
            ASSERT_EQ(pos_after_dec,bv_size);
        }
        for (size_t j = 0; j < B.size(); j++) {
            ASSERT_EQ(B[j], A[j]);
        }
    }
}


TEST(bit_stream, minbin_size)
{
    std::vector<uint64_t> A = {0,1,2,3,4,5};
    
    sdsl::bit_vector bv;
    {
        bit_ostream<sdsl::bit_vector> os(bv);
        os.put_minbin_int(0,6); ASSERT_EQ(os.tellp(),2ULL);
        os.put_minbin_int(1,6); ASSERT_EQ(os.tellp(),4ULL);
        os.put_minbin_int(2,6); ASSERT_EQ(os.tellp(),7ULL);
        os.put_minbin_int(3,6); ASSERT_EQ(os.tellp(),10ULL);
        os.put_minbin_int(4,6); ASSERT_EQ(os.tellp(),13ULL);
        os.put_minbin_int(5,6); ASSERT_EQ(os.tellp(),16ULL);
    }
    std::vector<uint64_t> B(A.size());
    {
        bit_istream<sdsl::bit_vector> is(bv);
        for(size_t j=0;j<A.size();j++) B[j] = is.get_minbin_int(6);
    }
    for (size_t j = 0; j < B.size(); j++) {
        ASSERT_EQ(B[j], A[j]);
    }
}

TEST(bit_stream, minbin)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    

    for (size_t i = 0; i < n; i++) {
        auto max_val = 256 + rand() % 12345678;
        std::uniform_int_distribution<uint64_t> dis(1, max_val);
        size_t len = 5000;
        std::vector<uint64_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            for(size_t j=0;j<len;j++) os.put_minbin_int(A[j],max_val);
        }
        std::vector<uint64_t> B(len);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            for(size_t j=0;j<len;j++) B[j] = is.get_minbin_int(max_val);
        }
        for (size_t j = 0; j < B.size(); j++) {
            ASSERT_EQ(B[j], A[j]);
        }
    }
}


TEST(factor_coder, factor_coder_blocked_subdict_zzz)
{
    const uint32_t factorization_blocksize = 64 * 1024;
    const uint64_t dict_size_bytes = 256 * 1024 * 1024;
    const uint64_t literal_threshold = 3;
    const uint64_t dict_segment_size_bytes = 1024;
    const uint64_t dict_page_size = 16 * dict_segment_size_bytes;
    const uint64_t num_pages_in_dict = dict_size_bytes / dict_page_size;
    block_factor_data bfd(factorization_blocksize);


    using p0_coder = factor_coder_blocked_subdict_zzz<literal_threshold,dict_page_size,num_pages_in_dict,dict_size_bytes,coder::elias_fano>; 

    size_t n = 200;
    sdsl::bit_vector bv;
    {
    bit_ostream<sdsl::bit_vector> os(bv);
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> num_fact_dist(100, 7000);
    std::lognormal_distribution<> fact_len_dist(2, 0.3);
    std::uniform_int_distribution<uint64_t> lit_sym_dist(1, 255);
    std::poisson_distribution<> offset_fact_dist(dict_size_bytes/4);
    for(size_t i=0;i<n;i++) {
        // generate data
        bfd.reset();
        bfd.num_factors = num_fact_dist(gen);
        for(size_t j=0;j<bfd.num_factors;j++) {
            auto flen = std::round(fact_len_dist(gen));
            bfd.lengths[j] = flen;
            if(flen <= literal_threshold) {
                for(size_t k=0;k<flen;k++) {
                    auto sym = lit_sym_dist(gen);
                    bfd.literals[bfd.num_literals++] = sym;
                }
            } else {
                // offset case
                auto offset = offset_fact_dist(gen);
                bfd.offsets[bfd.num_offsets++] = offset;
            }
        }

        // encode it 
        p0_coder c;
        {
            c.encode_block(os,bfd);
        }
    }
    }
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> num_fact_dist(100, 7000);
    std::lognormal_distribution<> fact_len_dist(2, 0.3);
    std::uniform_int_distribution<uint64_t> lit_sym_dist(1, 255);
    std::poisson_distribution<> offset_fact_dist(dict_size_bytes/4);
    bit_istream<sdsl::bit_vector> is(bv);
    for(size_t i=0;i<n;i++) {
        // generate data
        bfd.reset();
        bfd.num_factors = num_fact_dist(gen);
        for(size_t j=0;j<bfd.num_factors;j++) {
            auto flen = std::round(fact_len_dist(gen));
            bfd.lengths[j] = flen;
            if(flen <= literal_threshold) {
                for(size_t k=0;k<flen;k++) {
                    auto sym = lit_sym_dist(gen);
                    bfd.literals[bfd.num_literals++] = sym;
                }
            } else {
                // offset case
                auto offset = offset_fact_dist(gen);
                bfd.offsets[bfd.num_offsets++] = offset;
            }
        }

        // decode it 
        p0_coder c;
        block_factor_data bfd_recover(factorization_blocksize);
        {
            c.decode_block(is,bfd_recover,bfd.num_factors);
        }
        for(size_t j=0;j<bfd.num_offsets;j++) {
            ASSERT_EQ(bfd.offsets[j],bfd_recover.offsets[j]);
        }
        for(size_t j=0;j<bfd.num_literals;j++) {
            ASSERT_EQ(bfd.literals[j],bfd_recover.literals[j]);
        }
    }
}

TEST(factor_coder, factor_coder_blocked_subdict_zzz_ip)
{
    const uint32_t factorization_blocksize = 64 * 1024;
    const uint64_t dict_size_bytes = 256 * 1024 * 1024;
    const uint64_t literal_threshold = 3;
    const uint64_t dict_segment_size_bytes = 1024;
    const uint64_t dict_page_size = 16 * dict_segment_size_bytes;
    const uint64_t num_pages_in_dict = dict_size_bytes / dict_page_size;
    block_factor_data bfd(factorization_blocksize);


    using p0_coder = factor_coder_blocked_subdict_zzz<literal_threshold,dict_page_size,num_pages_in_dict,dict_size_bytes,coder::interpolative>; 

    size_t n = 200;
    sdsl::bit_vector bv;
    {
    bit_ostream<sdsl::bit_vector> os(bv);
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> num_fact_dist(100, 7000);
    std::lognormal_distribution<> fact_len_dist(2, 0.3);
    std::uniform_int_distribution<uint64_t> lit_sym_dist(1, 255);
    std::poisson_distribution<> offset_fact_dist(dict_size_bytes/4);
    for(size_t i=0;i<n;i++) {
        // generate data
        bfd.reset();
        bfd.num_factors = num_fact_dist(gen);
        for(size_t j=0;j<bfd.num_factors;j++) {
            auto flen = std::round(fact_len_dist(gen));
            bfd.lengths[j] = flen;
            if(flen <= literal_threshold) {
                for(size_t k=0;k<flen;k++) {
                    auto sym = lit_sym_dist(gen);
                    bfd.literals[bfd.num_literals++] = sym;
                }
            } else {
                // offset case
                auto offset = offset_fact_dist(gen);
                bfd.offsets[bfd.num_offsets++] = offset;
            }
        }

        // encode it 
        p0_coder c;
        {
            c.encode_block(os,bfd);
        }
    }
    }
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> num_fact_dist(100, 7000);
    std::lognormal_distribution<> fact_len_dist(2, 0.3);
    std::uniform_int_distribution<uint64_t> lit_sym_dist(1, 255);
    std::poisson_distribution<> offset_fact_dist(dict_size_bytes/4);
    bit_istream<sdsl::bit_vector> is(bv);
    for(size_t i=0;i<n;i++) {
        // generate data
        bfd.reset();
        bfd.num_factors = num_fact_dist(gen);
        for(size_t j=0;j<bfd.num_factors;j++) {
            auto flen = std::round(fact_len_dist(gen));
            bfd.lengths[j] = flen;
            if(flen <= literal_threshold) {
                for(size_t k=0;k<flen;k++) {
                    auto sym = lit_sym_dist(gen);
                    bfd.literals[bfd.num_literals++] = sym;
                }
            } else {
                // offset case
                auto offset = offset_fact_dist(gen);
                bfd.offsets[bfd.num_offsets++] = offset;
            }
        }

        // encode it 
        p0_coder c;
        block_factor_data bfd_recover(factorization_blocksize);
        {
            c.decode_block(is,bfd_recover,bfd.num_factors);
        }
        for(size_t j=0;j<bfd.num_offsets;j++) {
            ASSERT_EQ(bfd.offsets[j],bfd_recover.offsets[j]);
        }
        for(size_t j=0;j<bfd.num_literals;j++) {
            ASSERT_EQ(bfd.literals[j],bfd_recover.literals[j]);
        }
    }
}


int main(int argc, char* argv[])
{
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
