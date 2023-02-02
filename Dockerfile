FROM python:3.10

ARG TARGETPLATFORM
ARG TARGETARCH
ARG TARGETVARIANT
RUN printf "I'm building for TARGETPLATFORM=${TARGETPLATFORM}" \
    && printf ", TARGETARCH=${TARGETARCH}" \
    && printf ", TARGETVARIANT=${TARGETVARIANT} \n" \
    && printf "With uname -s : " && uname -s \
    && printf "and  uname -m : " && uname -m

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

# Setup the AWS Client (so we can copy S3 files to the container if needed)
WORKDIR /tmp

RUN case ${TARGETPLATFORM} in \
         "linux/amd64")  AWSCLI_FILE=https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip  ;; \
         "linux/arm64")  AWSCLI_FILE=https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip  ;; \
    esac && \
    curl "${AWSCLI_FILE}" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -f awscliv2.zip

# Create an application user
RUN useradd app_user --create-home

ARG APP_DIR=/opt/flight_sql

RUN mkdir --parents ${APP_DIR} && \
    chown app_user:app_user ${APP_DIR} && \
    chown --recursive app_user:app_user /usr/local

# Switch to a less privileged user...
USER app_user

WORKDIR ${APP_DIR}

RUN python3 -m venv ${APP_DIR}/venv && \
    echo ". ${APP_DIR}/venv/bin/activate" >> ~/.bashrc && \
    . ~/.bashrc && \
    pip install --upgrade pip

# Set the PATH so that the Python Virtual environment is referenced for subsequent RUN steps (hat tip: https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Copy the scripts directory into the image (we copy directory-by-directory in order to maximize Docker caching)
COPY --chown=app_user:app_user ./scripts ./scripts

# This version of Arrow was tested successfully and will be used by default
ARG ARROW_VERSION="apache-arrow-11.0.0"

# Build and install Arrow
RUN scripts/build_arrow.sh "${ARROW_VERSION}" "Y"

# This version of DuckDB was tested successfully and will be used by default
ARG DUCKDB_VERSION="v0.6.1"

# Build and install DuckDB
RUN scripts/build_duckdb.sh "${DUCKDB_VERSION}" "Y"

# Get the SQLite3 database file
RUN mkdir data && \
    wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O data/TPC-H-small.db

# Install Python requirements
COPY --chown=app_user:app_user ./requirements.txt ./
RUN pip install --requirement ./requirements.txt

# Create DuckDB database file
RUN python "scripts/create_duckdb_database_file.py" \
           --file-name="TPC-H-small.duckdb" \
           --file-path="data" \
           --overwrite-file=true \
           --scale-factor=0.01

# Build the Flight SQL application
COPY --chown=app_user:app_user ./CMakeLists.txt ./
COPY --chown=app_user:app_user ./src ./src
COPY --chown=app_user:app_user ./jwt-cpp ./jwt-cpp
WORKDIR ${APP_DIR}
RUN . ~/.bashrc && \
    mkdir build && \
    cd build && \
    cmake .. -GNinja -DCMAKE_PREFIX_PATH=${ARROW_HOME}/lib/cmake && \
    ninja

COPY --chown=app_user:app_user ./tls ./tls

WORKDIR ${APP_DIR}/scripts

EXPOSE 31337

ENTRYPOINT ./start_flight_sql.sh
