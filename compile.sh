#!/usr/bin/env bash

pushd commoncrawl_filter
RUSTFLAGS="-C target-cpu=native" cargo build --release
popd

pushd img_dl
RUSTFLAGS="-C target-cpu=native" cargo build --release
popd

cp commoncrawl_filter/target/release/commoncrawl_filter commoncrawl_filter_bin
cp img_dl/target/release/img_dl img_dl_bin
