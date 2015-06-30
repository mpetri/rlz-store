cmake_minimum_required(VERSION 2.8)
project(bzip2)

include_directories(
      ${CMAKE_CURRENT_SOURCE_DIR}
      )

add_library(bzip2 blocksort.c
    bzlib.c
    compress.c
    crctable.c
    decompress.c
    huffman.c
    randtable.c)


