#!/bin/bash
TEMP_DIR="../temp"

# check if temp directory exists
if [ ! -d "$TEMP_DIR" ]; then
    echo "$TEMP_DIR doesn't exist. Creating."
fi

# clone the repository
cd $TEMP_DIR
if [ ! -d "arrow" ]; then
    echo "Cloning Arrow."
    git clone https://github.com/apache/arrow.git
fi

cd arrow
mamba install -c conda-forge -y --file ci/conda_env_cpp.txt
mamba install -c conda-forge -y compilers
cd cpp && mkdir -p build && cd build && rm -rf *
cmake .. -GNinja -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
    -DARROW_FLIGHT=ON \
    -DARROW_FLIGHT_SQL=ON \
    -DARROW_JSON=ON \
    -DARROW_PARQUET=ON \
    -DARROW_CSV=ON \
    -DARROW_WITH_SNAPPY=ON
ninja install # Will install into your Conda prefix