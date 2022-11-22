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

# Copy the scripts directory into the image
COPY ./scripts ./scripts

# This version of Arrow was tested successfully and will be used by default
ARG ARROW_VERSION="apache-arrow-10.0.0"

# Build and install Arrow
RUN . ~/.bashrc && \
    scripts/build_arrow.sh "${ARROW_VERSION}"

# This version of DuckDB was tested successfully and will be used by default
ARG DUCKDB_VERSION="v0.6.0"

# Build and install DuckDB
RUN . ~/.bashrc && \
    scripts/build_duckdb.sh "${DUCKDB_VERSION}"

# Get the data
RUN mkdir data && \
    wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O data/TPC-H-small.db

# Install requirements
COPY ./requirements.txt ./
RUN . ~/.bashrc && \
    pip install --requirement ./requirements.txt

# Create duckdb database
RUN . ~/.bashrc && \
    scripts/get_duckdb_database.sh

# Build the Flight SQL application
COPY ./CMakeLists.txt ./
COPY ./src ./src
WORKDIR ${APP_DIR}
RUN . ~/.bashrc && \
    mkdir build && \
    cd build && \
    cmake .. -GNinja -DCMAKE_PREFIX_PATH=$ARROW_HOME/lib/cmake && \
    ninja

WORKDIR ${APP_DIR}/build

EXPOSE 31337

ENTRYPOINT ./flight_sql --backend=duckdb --database_file_path="../data" --database_file_name="TPC-H-small.duckdb"
