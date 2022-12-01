#!/bin/bash

set -e

# Be sure to "docker login" first..

docker buildx build --tag prmoorevoltron/flight-sql:latest --platform linux/arm64 --push .
