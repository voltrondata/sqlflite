#!/bin/bash

set -e
set -o pipefail

ARROW_VERSION=${ARROW_VERSION:-"apache-arrow-10.0.0"}
echo "Variable: ARROW_VERSION=${ARROW_VERSION}"

rm -rf arrow

# clone the repository
if [ ! -d "arrow" ]; then
    echo "Cloning Arrow."
    git clone --depth 1 https://github.com/apache/arrow.git --branch ${ARROW_VERSION}
fi

pushd arrow
git submodule update --init
popd

pip install -r arrow/python/requirements-build.txt
rm -rf dist
mkdir dist
export ARROW_HOME=$(pwd)/dist
export LD_LIBRARY_PATH=${ARROW_HOME}/lib:$LD_LIBRARY_PATH

# Add exports to the .bashrc for future sessions
echo "export ARROW_HOME=${ARROW_HOME}" >> ~/.bashrc
echo "export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}" >> ~/.bashrc

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

cmake -GNinja -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
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
        -DGTest_SOURCE=BUNDLED

ninja install
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

# Do some more Mac stuff if needed...
if [ "${OS}" == "Darwin" ]; then
  echo "Running Mac-specific PyArrow steps..."
  cp $ARROW_HOME/lib/*.* /usr/local/lib
fi
