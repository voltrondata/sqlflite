#!/bin/bash

set -eux

openssl genrsa -out root-ca.key 4096
openssl req -x509 -new -nodes \
        -subj "/C=US/ST=CA/O=MyOrg, Inc./CN=test" \
        -key root-ca.key -sha256 -days 10000 -out root-ca.pem

openssl genrsa -out cert0.key 4096
openssl req -new -sha256 -key cert0.key \
        -subj "/C=US/ST=CA/O=MyOrg, Inc./CN=localhost" \
        -out cert0.csr
# Convert to PKCS#1 for Java
openssl pkcs8 -in cert0.key -topk8 -nocrypt > cert0.pkcs1
openssl x509 -req -in cert0.csr -CA root-ca.pem -CAkey root-ca.key -CAcreateserial \
        -out cert0.pem -days 10000 -sha256

openssl genrsa -out cert1.key 4096
openssl req -new -sha256 -key cert1.key \
        -subj "/C=US/ST=CA/O=MyOrg, Inc./CN=localhost" \
        -out cert1.csr
openssl pkcs8 -in cert1.key -topk8 -nocrypt > cert1.pkcs1
openssl x509 -req -in cert1.csr -CA root-ca.pem -CAkey root-ca.key -CAcreateserial \
        -out cert1.pem -days 10000 -sha256
