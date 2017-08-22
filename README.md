
# Install Instructions

0. install libboost and liblzma-dev on linux
1. `git clone git@github.com:/mpetri/rlz-store.git`
2. `cd rlz-store`
3. `git submodule update --init --recursive`
3. `mkdir build`
4. `cmake ..`
5. `make`

# Creating a collection and index

Create a collection from a file `english.200MB`

`./create-collection.x -c ../collections/english200 -i english.200MB

Create the index

`./rlz-create.x -c ../collections/english200 -s 8 -t 15`

