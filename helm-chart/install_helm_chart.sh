#!/bin/bash

set -e

kubectl config set-context --current --namespace=sqlflite

helm upgrade demo \
     --install . \
     --namespace sqlflite \
     --create-namespace \
     --values values.yaml
