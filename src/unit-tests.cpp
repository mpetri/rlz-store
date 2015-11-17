#include "gtest/gtest.h"
#include "sdsl/int_vector.hpp"
#include "bit_streams.hpp"
#include "bit_coders.hpp"
#include <functional>
#include <random>

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

TEST(bit_stream, lzma)
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
        coder::lzma<2> c;
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

int main(int argc, char* argv[])
{
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}
