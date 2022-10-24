#!/bin/bash

set -e
set -o pipefail

echo "Environment variable: ARROW_VERSION=${ARROW_VERSION:-"apache-arrow-10.0.0"}"

# clone the repository
if [ ! -d "arrow" ]; then
    echo "Cloning Arrow."
    git clone --depth 1 https://github.com/apache/arrow.git --branch ${ARROW_VERSION}
fi

pushd arrow
git submodule update --init
popd

pip install -r arrow/python/requirements-build.txt
mkdir dist
export ARROW_HOME=$(pwd)/dist
export LD_LIBRARY_PATH=${ARROW_HOME}/lib:$LD_LIBRARY_PATH

# Add exports to the .bashrc for future sessions
echo "export ARROW_HOME=${ARROW_HOME}" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}" >> ~/.bashrc

#----------------------------------------------------------------------
# Build C++ library

mkdir arrow/cpp/build
pushd arrow/cpp/build
cmake -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_BUILD_TYPE=Debug \
        -DARROW_BUILD_TESTS=ON \
        -DARROW_COMPUTE=ON \
        -DARROW_CSV=ON \
        -DARROW_DATASET=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_FLIGHT=ON \
        -DARROW_FLIGHT_SQL=ON \
        -DARROW_HDFS=ON \
        -DARROW_JSON=ON \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_BROTLI=ON \
        -DARROW_WITH_BZ2=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_ZSTD=ON \
        -DPARQUET_REQUIRE_ENCRYPTION=ON \
        ..
make -j4
make install
popd

#----------------------------------------------------------------------
# Build and test Python library
pushd arrow/python

rm -rf build/  # remove any pesky pre-existing build directory

export CMAKE_PREFIX_PATH=${ARROW_HOME}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_DATASET=1
export PYARROW_PARALLEL=4
python setup.py develop
popd
