#!/bin/bash

set -e

# Be sure to login to AWS Console via Jumpcloud first..
# Set AWS env vars (example fake values shown below):
#export AWS_ACCESS_KEY_ID="Foo"
#export AWS_SECRET_ACCESS_KEY="Bar"
#export AWS_SESSION_TOKEN="Zee"

aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 795371563663.dkr.ecr.us-east-2.amazonaws.com

docker buildx build --tag 795371563663.dkr.ecr.us-east-2.amazonaws.com/datastax-poc-flight-sql:latest --platform linux/amd64,linux/arm64 --push .
