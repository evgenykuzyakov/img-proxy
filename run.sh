#!/bin/bash

mkdir -p logs

export PORT=3030
export REFERER="https://fetcher.com"
export IMAGE_RESCALE_URL_Thumbnail="https://imageproxy.com/thumb?url="
export IMAGE_RESCALE_URL_Large="https://imageproxy.com/large?url="
export RUST_LOG=warp=info,cache=info,fetch=info

cd $(dirname "$0")
DATE=$(date "+%Y_%m_%d")
cargo run --release 2>&1 | tee -a logs/logs_$DATE.txt
