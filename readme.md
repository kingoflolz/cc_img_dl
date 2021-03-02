# CommonCrawl/Creative Commons Image Downloader

A script for extracting URLs/license information, and downloading images from the output of
`commoncrawl_filter` (https://github.com/kingoflolz/commoncrawl_filter)

Currently, `dump_urls.py` tries to convert from page level data to image level data, deduplicating within a single WAT

# TODOs
- [ ] Infrastructure to run `dump_urls.py` across a glob of `.jsonl.wat.gz`s in parallel
- [ ] Shuffling and global deduplication
- [ ] Host checking, downloading, resizing/converting