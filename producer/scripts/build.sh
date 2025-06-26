#!/bin/bash

set -e

docker build -t demo-producer:$1 .

docker rm -f demo-producer

docker run -itd --name=demo-producer -v /home/ubuntu/repos/demo/producer/pg.json:/app/pg.json --network demo demo-producer:0.0.1
