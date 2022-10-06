#!/bin/bash

mkdir -p logs

export IMAGE_RESCALE_URL="https://imageproxy.com"

cd $(dirname "$0")
DATE=$(date "+%Y_%m_%d")
env RUST_LOG=imgs=info,cache=info,fetch=info cargo run --release 2>&1 | tee -a logs/logs_$DATE.txt
