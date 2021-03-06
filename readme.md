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

Then, use `sort_dedup.py` to perform URL level deduplication
```shell
# usage:
# python3 sort_dedup.py <threads> <input dir> <temp working dir> <output dir>
python3 sort_dedup.py 8 urls hash_clustered deduped_urls
```

# TODOs
- [x] Infrastructure to run `dump_urls.py` across a glob of `.jsonl.wat.gz`s in parallel
- [x] Shuffling and global deduplication
- [ ] Host checking, downloading, resizing/converting