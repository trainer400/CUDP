#!/bin/bash

# Enable errors to avoid continuing if something fails
set -e

# Create the build directory where to put all the cmake stuff
mkdir -p build

# Create cmake files
cd build
cmake .. && make -j$(nproc)