
# Install Instructions

0. install `libboost-all-dev` and `liblzma-dev` on Ubuntu or equivalent
1. `git clone git@github.com:/mpetri/rlz-store.git`
2. `cd rlz-store`
3. `git submodule update --init --recursive`
3. `mkdir build`
4. `cd build`
5. `cmake ..`
6. `make -j`

# Creating a collection and index

Create a collection from a file `english.200MB`

`./create-collection.x -c ../collections/english200 -i english.200MB`

Output:
```
2017-08-22 11:13:34,451 INFO :  Processing english.200MB
[======================================================================] 100 %
2017-08-22 11:13:39,710 INFO :  Replaced zeros = 0
2017-08-22 11:13:39,710 INFO :  Replaced ones = 0
2017-08-22 11:13:39,710 INFO :  Copied 209715200 bytes.
2017-08-22 11:13:39,905 INFO :  Writing document order file.
```


Create the index using a `8` MiB dictionary and `5` threads

`./rlz-create.x -c ../collections/english200 -s 8 -t 5`

Output:
```
2017-08-22 11:14:08,121 INFO :  Parsing command line arguments
2017-08-22 11:14:08,121 INFO :  Parsing collection directory ../collections/english.200MB/
2017-08-22 11:14:08,122 INFO :  Found input text with size 200 MiB
2017-08-22 11:14:08,122 INFO :  Create dictionary (dict_uniform_sample_budget-1024)
2017-08-22 11:14:08,122 INFO :  	Create dictionary with budget 8 MiB
2017-08-22 11:14:08,122 INFO :  	Dictionary samples = 8192
2017-08-22 11:14:08,122 INFO :  	Sample steps = 25600
2017-08-22 11:14:08,160 INFO :  	Writing dictionary.
2017-08-22 11:14:08,349 INFO :  	dict_uniform_sample_budget-1024 Total time = 0.227 sec
2017-08-22 11:14:08,356 INFO :  Dictionary hash before pruning '728721042'
2017-08-22 11:14:08,356 INFO :  Prune dictionary with dict_prune_none
2017-08-22 11:14:08,356 INFO :  	No dictionary pruning performed.
2017-08-22 11:14:08,356 INFO :  Dictionary after pruning '728721042'
2017-08-22 11:14:08,356 INFO :  Create/Load dictionary index
2017-08-22 11:14:08,356 INFO :  	Construct and store dictionary index
2017-08-22 11:14:08,357 INFO :  	Store reverse dictionary
2017-08-22 11:14:08,361 INFO :  	Construct index
2017-08-22 11:14:09,323 INFO :  Factorize text - 200 MiB (5 threads) - (factorizor-65536-l0-factor_select_first-factor_coder_blocked-t=3-zlib-9-zlib-9-zlib-9)
2017-08-22 11:14:14,314 INFO :     (1) FACTORS = 972902 BLOCKS = 160 F/B = 6080.64 Total F/B = 6080.64 SPEED = 2.00441 MiB/s (25%)
2017-08-22 11:14:14,369 INFO :     (4) FACTORS = 970562 BLOCKS = 160 F/B = 6066.01 Total F/B = 6066.01 SPEED = 1.98216 MiB/s (25%)
2017-08-22 11:14:14,445 INFO :     (0) FACTORS = 1016864 BLOCKS = 160 F/B = 6355.4 Total F/B = 6355.4 SPEED = 1.95274 MiB/s (25%)
2017-08-22 11:14:14,608 INFO :     (3) FACTORS = 1040484 BLOCKS = 160 F/B = 6503.02 Total F/B = 6503.02 SPEED = 1.89251 MiB/s (25%)
2017-08-22 11:14:14,883 INFO :     (2) FACTORS = 1074809 BLOCKS = 160 F/B = 6717.56 Total F/B = 6717.56 SPEED = 1.79921 MiB/s (25%)
2017-08-22 11:14:19,264 INFO :     (0) FACTORS = 963648 BLOCKS = 320 F/B = 6022.8 Total F/B = 6189.1 SPEED = 2.07555 MiB/s (50%)
2017-08-22 11:14:19,367 INFO :     (4) FACTORS = 989139 BLOCKS = 320 F/B = 6182.12 Total F/B = 6124.07 SPEED = 2.0012 MiB/s (50%)
2017-08-22 11:14:19,564 INFO :     (1) FACTORS = 1019185 BLOCKS = 320 F/B = 6369.91 Total F/B = 6225.27 SPEED = 1.90476 MiB/s (50%)
2017-08-22 11:14:19,848 INFO :     (2) FACTORS = 981985 BLOCKS = 320 F/B = 6137.41 Total F/B = 6427.48 SPEED = 2.0145 MiB/s (50%)
2017-08-22 11:14:19,945 INFO :     (3) FACTORS = 1030735 BLOCKS = 320 F/B = 6442.09 Total F/B = 6472.56 SPEED = 1.87371 MiB/s (50%)
2017-08-22 11:14:24,566 INFO :     (4) FACTORS = 987986 BLOCKS = 480 F/B = 6174.91 Total F/B = 6141.01 SPEED = 1.92382 MiB/s (75%)
2017-08-22 11:14:24,617 INFO :     (0) FACTORS = 1045156 BLOCKS = 480 F/B = 6532.23 Total F/B = 6303.48 SPEED = 1.86846 MiB/s (75%)
2017-08-22 11:14:24,874 INFO :     (1) FACTORS = 1045955 BLOCKS = 480 F/B = 6537.22 Total F/B = 6329.25 SPEED = 1.88324 MiB/s (75%)
2017-08-22 11:14:25,003 INFO :     (2) FACTORS = 998092 BLOCKS = 480 F/B = 6238.07 Total F/B = 6364.35 SPEED = 1.94024 MiB/s (75%)
2017-08-22 11:14:25,084 INFO :     (3) FACTORS = 1006081 BLOCKS = 480 F/B = 6288.01 Total F/B = 6411.04 SPEED = 1.94628 MiB/s (75%)
2017-08-22 11:14:29,740 INFO :     (0) FACTORS = 1016034 BLOCKS = 640 F/B = 6350.21 Total F/B = 6315.16 SPEED = 1.95198 MiB/s (100%)
2017-08-22 11:14:29,828 INFO :     (4) FACTORS = 1022341 BLOCKS = 640 F/B = 6389.63 Total F/B = 6203.17 SPEED = 1.90042 MiB/s (100%)
2017-08-22 11:14:30,132 INFO :     (1) FACTORS = 1057448 BLOCKS = 640 F/B = 6609.05 Total F/B = 6399.2 SPEED = 1.90223 MiB/s (100%)
2017-08-22 11:14:30,327 INFO :     (3) FACTORS = 1013531 BLOCKS = 640 F/B = 6334.57 Total F/B = 6391.92 SPEED = 1.90767 MiB/s (100%)
2017-08-22 11:14:30,336 INFO :     (2) FACTORS = 1069202 BLOCKS = 640 F/B = 6682.51 Total F/B = 6443.89 SPEED = 1.87512 MiB/s (100%)
2017-08-22 11:14:30,336 INFO :  Factorization time = 21.013 sec
2017-08-22 11:14:30,336 INFO :  Factorization speed = 9.51792 MB/s
2017-08-22 11:14:30,336 INFO :  Factorize done. (factorizor-65536-l0-factor_select_first-factor_coder_blocked-t=3-zlib-9-zlib-9-zlib-9)
2017-08-22 11:14:30,336 INFO :  Output factorization statistics
2017-08-22 11:14:30,337 INFO :  =====================================================================
2017-08-22 11:14:30,337 INFO :  text size          = 209715200 bytes (200 MB)
2017-08-22 11:14:30,337 INFO :  encoding size      = 90267296 bytes (86.0856 MB)
2017-08-22 11:14:30,337 INFO :  compression ratio  = 43.0428 %
2017-08-22 11:14:30,337 INFO :  space savings      = 56.9572 %
2017-08-22 11:14:30,337 INFO :  number of factors  = 20322139
2017-08-22 11:14:30,337 INFO :  bits per factor    = 35.5346
2017-08-22 11:14:30,337 INFO :  number of blocks   = 3200
2017-08-22 11:14:30,337 INFO :  avg factors/block  = 6350.67
2017-08-22 11:14:30,337 INFO :  =====================================================================
2017-08-22 11:14:30,337 INFO :  Merge factorized text blocks
2017-08-22 11:14:30,337 INFO :  	Rename offset 0 block to output files
2017-08-22 11:14:30,337 INFO :  	Copy block 1 factors/offsets/counts
2017-08-22 11:14:30,354 INFO :  	Copy block 2 factors/offsets/counts
2017-08-22 11:14:30,370 INFO :  	Copy block 3 factors/offsets/counts
2017-08-22 11:14:30,384 INFO :  	Copy block 4 factors/offsets/counts
2017-08-22 11:14:30,399 INFO :  	Delete temporary files
2017-08-22 11:14:30,408 INFO :  Create block map (block_map_uncompressed)
2017-08-22 11:14:30,408 INFO :  	Load block offsets from file
2017-08-22 11:14:30,408 INFO :  RLZ construction complete. time = 22 sec
2017-08-22 11:14:30,408 INFO :  Loading RLZ store into memory
2017-08-22 11:14:30,408 INFO :  	Load block map
2017-08-22 11:14:30,408 INFO :  	Load dictionary
2017-08-22 11:14:30,410 INFO :  	Determine text size
2017-08-22 11:14:30,410 INFO :  RLZ store ready
2017-08-22 11:14:30,410 INFO :  Verify that factorization is correct.
2017-08-22 11:14:31,387 INFO :  SUCCESS! Text sucessfully recovered.
```
