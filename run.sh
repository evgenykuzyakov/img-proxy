#!/bin/bash

mkdir -p logs

export PORT=3030
export IMAGE_RESCALE_URL="https://imageproxy.com"
export RUST_LOG=warp=info,cache=info,fetch=info

cd $(dirname "$0")
DATE=$(date "+%Y_%m_%d")
cargo run --release 2>&1 | tee -a logs/logs_$DATE.txt
