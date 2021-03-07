# Common Crawl Filter

Some quick and dirty code for downloading CC WAT files and parsing out images which are CC licensed, output format is 
gzip compressed jsonl.

This does not use asynchronous IO, but you don't need very many streams to saturate bandwidth or CPU.

# Build and run instructions (single file)
```shell
RUSTFLAGS="-C target-cpu=native" cargo build --release
cp target/release/commoncrawl_filter .
commoncrawl_filter http://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/segments/1610703495901.0/wat/CC-MAIN-20210115134101-20210115164101-00000.warc.wat.gz CC-MAIN-20210115134101-20210115164101-00000.warc.wat.jsonl.gz
```

# Run instructions (all WATs)
A python helper program is also provided which helps you run multiple instances of the downloader

```shell
# get urls for all WATs
python3 download_warc_urls.py

# download and process everything
# arugments are <threads> <url list> <output path>
# e.g.
python3 download_cc.py 8 indexes_1614468564_warc_urls.txt out_dir
```