#!/bin/sh

cp src/main/docker/Dockerfile target/

docker build -t catalogue-server -f target/Dockerfile target/.
