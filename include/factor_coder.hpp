#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "bit_coders.hpp"
#include "factor_data.hpp"

#include <sdsl/suffix_arrays.hpp>

struct coder_size_info {
    uint32_t literal_bytes = 0;
    uint32_t length_bytes = 0;
    uint32_t offset_bytes = 0;
    uint64_t subdict_bytes = 0;
    bool used_subdict = false;
}; 

/*
	encode factors in blocks.
 */
template <uint32_t t_literal_threshold = 1,
          class t_coder_literal = coder::fixed<32>,
          class t_coder_offset = coder::aligned_fixed<uint32_t>,
          class t_coder_len = coder::vbyte>
struct factor_coder_blocked {
    typedef typename sdsl::int_vector<>::size_type size_type;
    enum { literal_threshold = t_literal_threshold };
    t_coder_literal literal_coder;
    t_coder_offset offset_coder;
    t_coder_len len_coder;
    static std::string type()
    {
        return "factor_coder_blocked-t=" + std::to_string(t_literal_threshold)
               + "-" + t_coder_literal::type() + "-" + t_coder_offset::type() + "-" + t_coder_len::type();
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs,block_factor_data& bfd) const
    {
        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + bfd.num_factors, [](uint32_t& n) { n--; });
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        if (bfd.num_literals)
            literal_coder.encode(ofs, bfd.literals.data(), bfd.num_literals);
        if (bfd.num_offsets) {
            offset_coder.encode(ofs, bfd.offsets.data(), bfd.num_offsets);
        }
    }

    template <class t_ostream>
    void encode_block_output(t_ostream& ofs,block_factor_data& bfd,size_t block_id,const char* name) const
    {
        size_t total_before = ofs.tellp();       
        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + bfd.num_factors, [](uint32_t& n) { n--; });
        size_t before = ofs.tellp();
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        size_t len_size_bytes = (ofs.tellp() - before + 7)/8;
        
        before = ofs.tellp();
        if (bfd.num_literals)
            literal_coder.encode(ofs, bfd.literals.data(), bfd.num_literals);
        
        size_t literal_size_bytes = (ofs.tellp() - before + 7)/8;
            
        size_t offsets_before = ofs.tellp();
        if (bfd.num_offsets) {
            offset_coder.encode(ofs, bfd.offsets.data(), bfd.num_offsets);
        }
        size_t offset_size_bytes = (ofs.tellp() - offsets_before+7)/8;
        size_t total_size_bytes = (ofs.tellp() - total_before+7)/8;
        
        LOG(INFO) << name << ";"
                  << block_id << ";"
                  << len_size_bytes << ";"
                  << literal_size_bytes << ";"
                  << offset_size_bytes << ";"
                  << 0 << ";"
                  << bfd.num_offsets << ";"
                  << 0 << ";"
                  << 0 << ";"
                  << total_size_bytes;
    }

    template <class t_istream>
    coder_size_info decode_block(t_istream& ifs,block_factor_data& bfd, size_t num_factors) const
    {
        coder_size_info csi;
        bfd.num_factors = num_factors;

        auto len_pos = ifs.tellg();
        len_coder.decode(ifs, bfd.lengths.data(), num_factors);
        csi.length_bytes = (ifs.tellg() - len_pos +7)/8;

        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + num_factors, [](uint32_t& n) { n++; });
        bfd.num_literals = 0;
        auto num_literal_factors = 0;
        for (size_t i = 0; i < bfd.num_factors; i++) {
            if (bfd.lengths[i] <= literal_threshold) {
                bfd.num_literals += bfd.lengths[i];
                num_literal_factors++;
            }
        }
        if (bfd.num_literals) {
            auto lit_pos = ifs.tellg();
            literal_coder.decode(ifs, bfd.literals.data(), bfd.num_literals);
            csi.literal_bytes = (ifs.tellg() - lit_pos + 7)/8;
        }
        bfd.num_offsets = bfd.num_factors - num_literal_factors;
        if (bfd.num_offsets) {
            auto off_pos = ifs.tellg();
            offset_coder.decode(ifs, bfd.offsets.data(), bfd.num_offsets);
            csi.offset_bytes = (ifs.tellg() - off_pos + 7)/8;
        }
        return csi;
    }
};

template <uint32_t t_literal_threshold = 1,
          uint32_t t_page_size = 16 * 1024,
          uint32_t t_total_pages = 16 * 1024,
          uint64_t t_dict_size = 256 * 1024 * 1024,
          class t_coder_pagenums = coder::elias_fano>
struct factor_coder_blocked_subdict_zzz {
    typedef typename sdsl::int_vector<>::size_type size_type;
    enum { literal_threshold = t_literal_threshold };
    coder::zlib<9> literal_coder;
    coder::zlib<9> offset_coder;
    coder::zlib<9> len_coder;
    t_coder_pagenums page_coder;
    mutable std::vector<uint64_t> page_offsets;
    mutable std::vector<uint32_t> mapped_offsets;
    mutable sdsl::bit_vector test_mapped_buf;
    mutable sdsl::bit_vector test_unmapped_buf;
    mutable bit_ostream<sdsl::bit_vector> test_mapped_buf_stream;
    mutable bit_ostream<sdsl::bit_vector> test_unmapped_buf_stream;
    const uint8_t page_size_log2 = utils::CLog2<t_page_size>();
    const uint8_t dict_size_log2 = utils::CLog2<t_dict_size>();
    
    factor_coder_blocked_subdict_zzz() : test_mapped_buf_stream(test_mapped_buf) , test_unmapped_buf_stream(test_unmapped_buf) {
        
    }
    
    static std::string type()
    {
        return "factor_coder_blocked_subdict-zzz-t=" + std::to_string(t_literal_threshold) + "-p=" + std::to_string(t_page_size)+ "-tp=" + std::to_string(t_total_pages)
               + "-" + coder::zlib<9>::type() + "-" + coder::zlib<9>::type() + "-" + coder::zlib<9>::type() + "-" + t_coder_pagenums::type();
    }

    template <class t_ostream>
    void encode_block(t_ostream& ofs, block_factor_data& bfd) const
    {
        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + bfd.num_factors, [](uint32_t& n) { n--; });
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        if (bfd.num_literals)
            literal_coder.encode(ofs, bfd.literals.data(), bfd.num_literals);

        if (bfd.num_offsets) {
            if(page_offsets.size() < bfd.num_offsets) page_offsets.resize(bfd.num_offsets);
            // (1) determine page offsets
            for(size_t i=0;i<bfd.num_offsets;i++) {
                page_offsets[i] = bfd.offsets[i] >> page_size_log2;
            }
            // (2) determine unique offsets
            std::sort(page_offsets.begin(),page_offsets.begin()+bfd.num_offsets);
            auto end = std::unique(page_offsets.begin(),page_offsets.begin()+bfd.num_offsets);
            size_t num_pages_in_block = std::distance(page_offsets.begin(),end);

            // (3) map offsets
            if(mapped_offsets.size() < bfd.num_offsets) mapped_offsets.resize(bfd.num_offsets);
            for(size_t i=0;i<bfd.num_offsets;i++) {
                auto page_nr = bfd.offsets[i] >> page_size_log2;
                auto in_page_offset = bfd.offsets[i]&sdsl::bits::lo_set[page_size_log2];
                auto new_page_itr = std::lower_bound(page_offsets.begin(),end, page_nr);
                auto new_page_nr = std::distance(page_offsets.begin(),new_page_itr);
                mapped_offsets[i] = (new_page_nr << page_size_log2) + in_page_offset;
            }

            // (4) estimate cost to encode
            auto compressed_pagetable_size = page_coder.determine_size(page_offsets.data(),num_pages_in_block,t_total_pages);
            test_mapped_buf_stream.seek(0);
            offset_coder.encode(test_mapped_buf_stream,mapped_offsets.data(),bfd.num_offsets);
            auto offset_cost_mapped = test_mapped_buf_stream.tellp();
            auto compressed_size = offset_cost_mapped + compressed_pagetable_size;
            test_unmapped_buf_stream.seek(0);
            offset_coder.encode(test_unmapped_buf_stream,bfd.offsets.data(),bfd.num_offsets);
            auto uncompressed_size = test_unmapped_buf_stream.tellp();
            
            if(compressed_size < uncompressed_size) {
                // (4a) encode page numbers
                ofs.put_int(num_pages_in_block,16);
                page_coder.encode(ofs,page_offsets.data(),num_pages_in_block,t_total_pages);

                // (4c) write already encoded zlib data
                ofs.expand_if_needed(offset_cost_mapped+8);
                ofs.align8(); // align to bytes if needed
                uint8_t* out_buf8 = (uint8_t*)ofs.cur_data8();
                test_mapped_buf_stream.seek(0);
                uint8_t* in_buf8 = (uint8_t*)test_mapped_buf_stream.cur_data8();
                auto out_bytes = offset_cost_mapped / 8;
                for(size_t i=0;i<out_bytes;i++) *out_buf8++ = *in_buf8++;
                ofs.skip(offset_cost_mapped);
            } else {
                // regular uncompressed encoding
                ofs.put_int(0,16);
                ofs.expand_if_needed(uncompressed_size+8);
                ofs.align8(); // align to bytes if needed
                uint8_t* out_buf8 = (uint8_t*)ofs.cur_data8();
                test_unmapped_buf_stream.seek(0);
                uint8_t* in_buf8 = (uint8_t*)test_unmapped_buf_stream.cur_data8();
                auto out_bytes = uncompressed_size / 8;
                for(size_t i=0;i<out_bytes;i++) *out_buf8++ = *in_buf8++;
                ofs.skip(uncompressed_size);
            }
        }
    }


    template <class t_ostream>
    void encode_block_output(t_ostream& ofs,block_factor_data& bfd,size_t block_id,const char* name) const
    {
        size_t total_before = ofs.tellp();
        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + bfd.num_factors, [](uint32_t& n) { n--; });
        size_t before = ofs.tellp();
        len_coder.encode(ofs, bfd.lengths.data(), bfd.num_factors);
        size_t len_size_bytes = (ofs.tellp() - before + 7)/8;
        
        before = ofs.tellp();
        if (bfd.num_literals) {
            literal_coder.encode(ofs, bfd.literals.data(), bfd.num_literals);
        }
        size_t literal_size_bytes = (ofs.tellp() - before + 7)/8;
        
        size_t offsets_before = ofs.tellp();
        size_t page_table_size_bytes = 0;
        bool use_page_table = false;
        size_t num_pages_in_block = 0;

        if (bfd.num_offsets) {
            if(page_offsets.size() < bfd.num_offsets) page_offsets.resize(bfd.num_offsets);
            // (1) determine page offsets
            for(size_t i=0;i<bfd.num_offsets;i++) {
                page_offsets[i] = bfd.offsets[i] >> page_size_log2;
            }
            // (2) determine unique offsets
            std::sort(page_offsets.begin(),page_offsets.begin()+bfd.num_offsets);
            auto end = std::unique(page_offsets.begin(),page_offsets.begin()+bfd.num_offsets);
            num_pages_in_block = std::distance(page_offsets.begin(),end);

            // (3) map offsets
            if(mapped_offsets.size() < bfd.num_offsets) mapped_offsets.resize(bfd.num_offsets);
            for(size_t i=0;i<bfd.num_offsets;i++) {
                auto page_nr = bfd.offsets[i] >> page_size_log2;
                auto in_page_offset = bfd.offsets[i]&sdsl::bits::lo_set[page_size_log2];
                auto new_page_itr = std::lower_bound(page_offsets.begin(),end, page_nr);
                auto new_page_nr = std::distance(page_offsets.begin(),new_page_itr);
                mapped_offsets[i] = (new_page_nr << page_size_log2) + in_page_offset;
            }

            // (4) estimate cost to encode
            auto compressed_pagetable_size = page_coder.determine_size(page_offsets.data(),num_pages_in_block,t_total_pages);
            test_mapped_buf_stream.seek(0);
            offset_coder.encode(test_mapped_buf_stream,mapped_offsets.data(),bfd.num_offsets);
            auto offset_cost_mapped = test_mapped_buf_stream.tellp();
            auto compressed_size = offset_cost_mapped + compressed_pagetable_size;
            test_unmapped_buf_stream.seek(0);
            offset_coder.encode(test_unmapped_buf_stream,bfd.offsets.data(),bfd.num_offsets);
            auto uncompressed_size = test_unmapped_buf_stream.tellp();
            
            if(compressed_size < uncompressed_size) {
                // (4a) encode page numbers
                ofs.put_int(num_pages_in_block,16);
                
                use_page_table = true;
                size_t pagetable_before = ofs.tellp();
                page_coder.encode(ofs,page_offsets.data(),num_pages_in_block,t_total_pages);
                page_table_size_bytes = (ofs.tellp() - pagetable_before + 7)/8;

                // (4c) write already encoded zlib data
                ofs.expand_if_needed(offset_cost_mapped+8);
                ofs.align8(); // align to bytes if needed
                uint8_t* out_buf8 = (uint8_t*)ofs.cur_data8();
                test_mapped_buf_stream.seek(0);
                uint8_t* in_buf8 = (uint8_t*)test_mapped_buf_stream.cur_data8();
                auto out_bytes = offset_cost_mapped / 8;
                for(size_t i=0;i<out_bytes;i++) *out_buf8++ = *in_buf8++;
                ofs.skip(offset_cost_mapped);
            } else {
                // regular uncompressed encoding
                ofs.put_int(0,16);
                ofs.expand_if_needed(uncompressed_size+8);
                ofs.align8(); // align to bytes if needed
                uint8_t* out_buf8 = (uint8_t*)ofs.cur_data8();
                test_unmapped_buf_stream.seek(0);
                uint8_t* in_buf8 = (uint8_t*)test_unmapped_buf_stream.cur_data8();
                auto out_bytes = uncompressed_size / 8;
                for(size_t i=0;i<out_bytes;i++) *out_buf8++ = *in_buf8++;
                ofs.skip(uncompressed_size);
            }
            
        }
        size_t offset_size_bytes = (ofs.tellp() - offsets_before + 7)/8;
        
        size_t total_size_bytes = (ofs.tellp() - total_before + 7)/8;
        
        LOG(INFO) << name << ";"
                  << block_id << ";"
                  << len_size_bytes << ";"
                  << literal_size_bytes << ";"
                  << offset_size_bytes << ";"
                  << page_table_size_bytes << ";"
                  << bfd.num_offsets << ";"
                  << num_pages_in_block << ";"
                  << use_page_table << ";"
                  << total_size_bytes;
    }

    template <class t_istream>
    coder_size_info decode_block(t_istream& ifs,block_factor_data& bfd, size_t num_factors) const
    {
        coder_size_info csi;
        bfd.num_factors = num_factors;

        auto len_pos = ifs.tellg();
        // LOG(INFO) << "decode lens at " << len_pos;
        len_coder.decode(ifs, bfd.lengths.data(), num_factors);
        csi.length_bytes = (ifs.tellg() - len_pos + 7)/8;

        std::for_each(bfd.lengths.begin(), bfd.lengths.begin() + num_factors, [](uint32_t& n) { n++; });
        bfd.num_literals = 0;
        auto num_literal_factors = 0;
        for (size_t i = 0; i < bfd.num_factors; i++) {
            if (bfd.lengths[i] <= literal_threshold) {
                bfd.num_literals += bfd.lengths[i];
                num_literal_factors++;
            }
        }
        if (bfd.num_literals) {
            auto lit_pos = ifs.tellg();
            literal_coder.decode(ifs, bfd.literals.data(), bfd.num_literals);
            csi.literal_bytes = (ifs.tellg() - lit_pos + 7)/8;
        }
        bfd.num_offsets = bfd.num_factors - num_literal_factors;
        if(mapped_offsets.size() < bfd.num_offsets) mapped_offsets.resize(bfd.num_offsets);
        auto off_pos = ifs.tellg();
        if (bfd.num_offsets) {
            auto num_pages_in_block = ifs.get_int(16);
            if(num_pages_in_block) {
                csi.used_subdict = true;
                if(page_offsets.size() < bfd.num_offsets) page_offsets.resize(bfd.num_offsets);
                // decode page table
                csi.subdict_bytes = ifs.tellg();
                page_coder.decode(ifs,page_offsets.data(),num_pages_in_block,t_total_pages);
                csi.subdict_bytes = (ifs.tellg() - csi.subdict_bytes + 7)/8;
                // decode offsets
                offset_coder.decode(ifs, bfd.offsets.data(), bfd.num_offsets);
                for(size_t i=0;i<bfd.num_offsets;i++) {
                    auto page_nr = bfd.offsets[i] >> page_size_log2;
                    auto in_page_offset = bfd.offsets[i]&sdsl::bits::lo_set[page_size_log2];
                    bfd.offsets[i] = (page_offsets[page_nr] << page_size_log2) + in_page_offset;
                }
            } else {
                csi.used_subdict = false;
                offset_coder.decode(ifs, bfd.offsets.data(), bfd.num_offsets);
            }
            
        }
        csi.offset_bytes = (ifs.tellg() - off_pos + 7)/8;
        return csi;
    }
};


