# CommonCrawl/Creative Commons Image Downloader

A script for extracting URLs/license information from CommonCrawl WATs and downloading/resizing these images

# Compile instructions

To compile the rust components (`commoncrawl_filter` and `img_dl`), run `./compile.sh` when in the root of the repo.

# Run instructions

Note all estimates are very rough, could easily be off by a factor of 2 (but should be the right OOM at least...)

First, to extract relevant data from Common Crawl WAT files. (~1.2PB ingress, ~500GB output, ~100 CPU days)

```shell
# get urls for all WATs
python3 download_warc_urls.py

# download and process all WATs
# Usage:
# python3 download_cc.py <threads> <url list> <output path>
python3 download_cc.py 8 indexes_1614468564_warc_urls.txt out_dir
```

Then use `dump_urls.py` to create image level metadata from page level metadata (~500GB input, ~250GB output, ~10 CPU days)
```shell
# usage:
# python3 dump_urls.py <threads> <input dir> <output dir (created automatically)>
python3 dump_urls.py 8 crawl urls
```

Use `sort_dedup.py` to perform URL level deduplication (~250GB input, ~400GB output, ~500GB scratch space, ~15 CPU days)
```shell
# usage:
# python3 sort_dedup.py <threads> <input dir> <temp working dir> <output dir>
python3 sort_dedup.py 8 urls hash_clustered deduped_urls
```

Use `download_images.py` (which calls `img_dl`) to actually download the data (~500TB ingress, ~500TB output, ~200 CPU days)
```shell
# usage:
# python3 download_images.py <threads> <input dir> <error dir> <image output dir>
python3 download_images.py 8 deduped_urls errors images

# to retry failed downloads after a complete run, use the errors directory as a new input dir. i.e.
python3 download_images.py 8 errors new_errors images

# and again later perhaps
python3 download_images.py 8 new_errors new_new_errors images

# etc etc repeat until satisfied
```

Use `file_convert.py` to convert all images to jpeg, resize if too large, discard if too small (~500TB input, ~200TB output, ~100 CPU days)
```shell
# usage:
# python3 file_convert.py <threads> <downloaded images> <deduped URL dir> <image output> <label output dir>
python3 8 images deduped_urls converted_images labels
```

# TODOs
- [ ] Additional filtering