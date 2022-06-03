#!/bin/bash

set -e
set -o pipefail

# Ensure that CONDA_PREFIX is set
echo "Environment variable: CONDA_PREFIX=${CONDA_PREFIX:?is not set - aborting!}"
echo "Environment variable: ARROW_COMMIT_HASH=${ARROW_COMMIT_HASH:-27e4bc16614f36857e1cdd491eba3fe3ec03d25e}"

TEMP_DIR="../temp"

# check if temp directory exists
if [ ! -d "$TEMP_DIR" ]; then
    echo "$TEMP_DIR doesn't exist. Creating."
    mkdir "$TEMP_DIR"
fi

# clone the repository
cd $TEMP_DIR
if [ ! -d "arrow" ]; then
    echo "Cloning Arrow."
    git clone https://github.com/apache/arrow.git
fi

cd arrow
git reset --hard ${ARROW_COMMIT_HASH}
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