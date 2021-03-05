# CommonCrawl/Creative Commons Image Downloader

A script for extracting URLs/license information, and downloading images from the output of
`commoncrawl_filter` (https://github.com/kingoflolz/commoncrawl_filter)

# Run instructions

First, use `dump_urls.py` to create image level metadata from page level metadata
```shell
# usage:
# python3 dump_urls.py <threads> <input dir> <output dir (created automatically)>
python3 dump_urls.py 8 crawl urls
```

Next: TODO

# TODOs
- [x] Infrastructure to run `dump_urls.py` across a glob of `.jsonl.wat.gz`s in parallel
- [ ] Shuffling and global deduplication
- [ ] Host checking, downloading, resizing/converting