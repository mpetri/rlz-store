
# Install Instructions

1. `git clone git@github.com:/unimelbIR/rlz-store.git`
2. `cd rlz-store`
3. `git submodule update --init --recursive`
3. `mkdir build`
4. `cmake ..`
5. `make`

# Creating an index

1. `cd build`
2. `make`
3. `./rlz-create.x -c ../collections/<name of col> -f -v`

# Creating a collection and index

Create a collection from a file 

`./create-sdsl-input.x -c ../collections/<name> -i <input file>`

Create the index

`./rlz-create.x -c ../collections/<name> -f -v`

