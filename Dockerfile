ARG BUILD_PLATFORM
FROM --platform=${BUILD_PLATFORM} python:3.10

# Install base utilities
RUN apt-get update && \
    apt-get dist-upgrade --yes && \
    apt-get install -y \
    build-essential \
    cmake \
    wget \
    gcc \
    git \
    ninja-build \
    libboost-all-dev \
    vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG APP_DIR=/opt/flight_sql

RUN mkdir --parents ${APP_DIR} &&\
    cd ${APP_DIR} && \
    python3 -m venv ${APP_DIR}/venv && \
    echo ". ${APP_DIR}/venv/bin/activate" >> ~/.bashrc && \
    . ~/.bashrc && \
    pip install --upgrade pip

WORKDIR ${APP_DIR}

# Copy the source code into the image
COPY . ./

# This version of Arrow was tested successfully and will be used by default
ARG ARG_ARROW_VERSION="apache-arrow-10.0.0"
ENV ARROW_VERSION=${ARG_ARROW_VERSION}

# Build and install Arrow
RUN . ~/.bashrc && \
    scripts/build_arrow.sh

# This version of DuckDB was tested successfully and will be used by default
ARG ARG_DUCKDB_VERSION="v0.5.1"
ENV DUCKDB_VERSION=${ARG_DUCKDB_VERSION}

# Build and install DuckDB
RUN . ~/.bashrc && \
    scripts/build_duckdb.sh

# Get the data
RUN mkdir data && \
    wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O data/TPC-H-small.db

# Install requirements
RUN . ~/.bashrc && \
    pip install --requirement ./requirements.txt

# Create duckdb database
RUN . ~/.bashrc && \
    scripts/get_duckdb_database.sh

# Build the sqlite
WORKDIR ${APP_DIR}
RUN . ~/.bashrc && \
    mkdir build && \
    cd build && \
    cmake .. -GNinja -DCMAKE_PREFIX_PATH=$ARROW_HOME/lib/cmake && \
    ninja

WORKDIR ${APP_DIR}/build
