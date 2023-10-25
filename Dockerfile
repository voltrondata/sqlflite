FROM python:3.11.6

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
    libsqlite3-dev \
    sqlite3 \
    vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Apache Arrow (per https://arrow.apache.org/install/)
RUN apt update && \
    apt install -y -V ca-certificates lsb-release wget && \
    wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
    apt update && \
    apt install -y -V \
       libarrow-dev \
       libarrow-glib-dev \
       libarrow-dataset-dev \
       libarrow-dataset-glib-dev \
       libarrow-acero-dev \
       libarrow-flight-dev \
       libarrow-flight-glib-dev \
       libarrow-flight-sql-dev \
       libarrow-flight-sql-glib-dev \
       libgandiva-dev \
       libgandiva-glib-dev \
       libparquet-dev \
       libparquet-glib-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the scripts directory into the image (we copy directory-by-directory in order to maximize Docker caching)
COPY --chown=app_user:app_user ./scripts ./scripts

# Build/Install DuckDB
ARG DUCKDB_VERSION="v0.9.1"
RUN scripts/build_duckdb.sh ${DUCKDB_VERSION} "Y"

WORKDIR /tmp

# Setup the AWS Client (so we can copy S3 files to the container if needed)
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
    pip install --upgrade pip setuptools wheel

# Set the PATH so that the Python Virtual environment is referenced for subsequent RUN steps (hat tip: https://pythonspeed.com/articles/activate-virtualenv-dockerfile/)
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Copy the scripts directory into the image (we copy directory-by-directory in order to maximize Docker caching)
COPY --chown=app_user:app_user ./scripts ./scripts

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
    ninja && \
    mv flight_sql /usr/local/bin

COPY --chown=app_user:app_user ./tls ./tls

EXPOSE 31337

# Run a test to ensure that the server works...
RUN scripts/test_flight_sql.sh

ENTRYPOINT scripts/start_flight_sql.sh
