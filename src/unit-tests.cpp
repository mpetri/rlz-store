#include "gtest/gtest.h"
#include "sdsl/int_vector.hpp"
#include "bit_streams.hpp"
#include "bit_coders.hpp"
#include <functional>
#include <random>

#include "easylogging++.h"

#include "utils.hpp"

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
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            os.encode<coder::vbyte>(A.begin(), n);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            is.decode<coder::vbyte>(B.begin(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(bit_stream, fix_width_int)
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
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            os.write_int(A.begin(), n, 7);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            is.get_int(B.begin(), n, 7);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(bit_stream, gamma)
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
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            os.encode<coder::elias_gamma>(A.begin(), last);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            is.decode<coder::elias_gamma>(B.begin(), n);
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
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            os.encode<coder::zlib<6> >(A.begin(), last);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            is.decode<coder::zlib<6> >(B.begin(), n);
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
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            os.encode<coder::lzma<2> >(A.begin(), last);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            is.decode<coder::lzma<2> >(B.begin(), n);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}


TEST(bit_stream, u32a)
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
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            os.encode< coder::u32a >(A.begin(), last);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            is.decode< coder::u32a >(B.begin(), n);
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
