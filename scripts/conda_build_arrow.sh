#!/bin/bash

set -e
set -o pipefail

ARROW_VERSION=${1:-"apache-arrow-10.0.1"}
REMOVE_SOURCE_FILES=${2:-"N"}

echo "Variable: ARROW_VERSION=${ARROW_VERSION}"

rm -rf arrow

# clone the repository
if [ ! -d "arrow" ]; then
    echo "Cloning Arrow."
    git clone --depth 1 https://github.com/apache/arrow.git --branch ${ARROW_VERSION}
fi

pushd arrow
git submodule update --init
export ARROW_TEST_DATA="${PWD}/testing/data"
rm -rf .git
popd

pip install -r arrow/python/requirements-build.txt

#----------------------------------------------------------------------
# Build C++ library

pushd arrow/cpp

# Do some Mac stuff if needed...
OS=$(uname)
if [ "${OS}" == "Darwin" ]; then
  echo "Running Mac-specific setup steps..."
  brew update && brew bundle --file=Brewfile
  export MACOSX_DEPLOYMENT_TARGET="12.0"
fi

cmake -GNinja \
        -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_BUILD_TYPE=Debug \
        -DARROW_BUILD_TESTS=ON \
        -DARROW_COMPUTE=OFF \
        -DARROW_CSV=ON \
        -DARROW_DATASET=ON \
        -DARROW_FILESYSTEM=ON \
        -DARROW_FLIGHT=ON \
        -DARROW_FLIGHT_SQL=ON \
        -DARROW_HDFS=OFF \
        -DARROW_JSON=OFF \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_BROTLI=OFF \
        -DARROW_WITH_BZ2=OFF \
        -DARROW_WITH_LZ4=OFF \
        -DARROW_WITH_SNAPPY=OFF \
        -DARROW_WITH_ZLIB=OFF \
        -DARROW_WITH_ZSTD=OFF \
        -DPARQUET_REQUIRE_ENCRYPTION=ON \
        -DGTest_SOURCE=BUNDLED

ninja install
popd

#----------------------------------------------------------------------
# Build and test Python library
pushd arrow/python

rm -rf build/  # remove any pesky pre-existing build directory

export CMAKE_PREFIX_PATH=${CONDA_PREFIX}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_DATASET=1
export PYARROW_PARALLEL=4
python setup.py develop
popd

# # Do some more Mac stuff if needed...
# if [ "${OS}" == "Darwin" ]; then
#   echo "Running Mac-specific PyArrow steps..."
#   cp $ARROW_HOME/lib/*.* /usr/local/lib
# fi

# # Remove source files
# if [ "${REMOVE_SOURCE_FILES}" == "Y" ]; then
#   echo "Removing Arrow source files..."
#   rm -rf ./arrow
# fi
