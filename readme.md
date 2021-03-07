# CommonCrawl/Creative Commons Image Downloader

A script for extracting URLs/license information from CommonCrawl WATs and downloading/resizing these images

# Run instructions

First, use `commoncrawl_filter` to extract relevant data from Common Crawl WAT files, see folder for more details.

Then use `dump_urls.py` to create image level metadata from page level metadata
```shell
# usage:
# python3 dump_urls.py <threads> <input dir> <output dir (created automatically)>
python3 dump_urls.py 8 crawl urls
```

Use `sort_dedup.py` to perform URL level deduplication
```shell
# usage:
# python3 sort_dedup.py <threads> <input dir> <temp working dir> <output dir>
python3 sort_dedup.py 8 urls hash_clustered deduped_urls
```

Finally, use `img_dl` to actually download the data
```shell
TODO
```

# TODOs
- [ ] Host checking, downloading, resizing/converting
- [ ] Additional filtering