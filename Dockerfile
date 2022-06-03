ARG BUILD_PLATFORM
FROM --platform=${BUILD_PLATFORM} ubuntu:20.04

# Install base utilities
RUN apt-get update && \
    apt-get install -y \
    build-essential \
    wget \
    git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Re-use the BUILD_PLATFORM arg b/c all args are reset after FROM statements!
ARG BUILD_PLATFORM

# Install miniconda
ARG CONDA_INSTALL_DIR=/opt/conda
ENV CONDA_DIR=${CONDA_INSTALL_DIR}

RUN echo -e "CONDA_DIR=${CONDA_DIR}"; \
    echo -e "BUILD_PLATFORM=${BUILD_PLATFORM}"; \
    if [ "${BUILD_PLATFORM}" = "linux/arm64" ]; \
    then export MINICONDA_PLATFORM="aarch64"; \
    else export MINICONDA_PLATFORM="x86_64"; \
    fi; \
    wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${MINICONDA_PLATFORM}.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p ${CONDA_DIR}

# Put conda in path so we can use conda activate
ENV PATH=$CONDA_DIR/bin:$PATH

ENV APP_DIR=/opt/flight_sql

# Install mamba and create a new environment
RUN . ~/.bashrc && \
    conda init bash && \
    conda install --yes --channel conda-forge mamba && \
    mkdir --parents ${APP_DIR}

WORKDIR ${APP_DIR}

# Copy the source code into the image
COPY . ./

WORKDIR scripts

# This version of Arrow was tested successfully and will be used by default
ARG ARG_ARROW_COMMIT_HASH="27e4bc16614f36857e1cdd491eba3fe3ec03d25e"
ENV ARROW_COMMIT_HASH=${ARG_ARROW_COMMIT_HASH}

# Build and install Arrow
RUN . ~/.bashrc && \
    ./install_arrow.sh

# This version of DuckDB was tested successfully and will be used by default
ARG ARG_DUCKDB_COMMIT_HASH="e9957d10ff11d32feb78f1601109a3c8a9f0578f"
ENV DUCKDB_COMMIT_HASH=${ARG_DUCKDB_COMMIT_HASH}

# Build and install DuckDB
RUN . ~/.bashrc && \
    ./install_duckdb.sh

# Get the data
RUN mkdir ../data && \
    wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O ../data/TPC-H-small.db

# Install requirements
RUN pip install -r ../requirements.txt

# Create duckdb database
RUN . ~/.bashrc && \
    ./get_duckdb_database.sh

# Build the example
WORKDIR ${APP_DIR}
RUN . ~/.bashrc && \
    mkdir build && \
    cd build && \
    cmake .. -GNinja -DCMAKE_PREFIX_PATH=$CONDA_PREFIX/lib/cmake/arrow && \
    ninja

WORKDIR ${APP_DIR}/build
