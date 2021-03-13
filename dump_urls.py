import gzip
import hashlib
import json
import sys
from glob import glob
from multiprocessing import Pool, set_start_method
from pathlib import Path
from urllib.parse import urlparse, urljoin

from tqdm import tqdm


def canonicalize_wikimedia(url):
    path = url.path.split("/")

    if "thumb" in path:
        path = path[:-1]
        path.remove("thumb")

    path = "/".join(path)

    return url._replace(path=path, scheme="http", params='', query='', fragment='')


def canonicalize_wp(url):
    target_url = url.path[1:]
    return urlparse(target_url)


def canonicalize_ytimg(url):
    path = url.path
    larger_suffixes = {"sddefault",  # 640
                       "maxresdefault",  # original
                       }

    *path, fname = path.split("/")
    option, _ = fname.split(".")

    if option not in larger_suffixes:
        option = "0"

    fname = f"{option}.jpg"
    path = "/".join(path + [fname])

    return url._replace(path=path, netloc="i.ytimg.com", scheme="http", params='', query='', fragment='')


# replace all suffixes with default (500x500) unless its larger
def canonicalize_flickr(url):
    path = url.path

    larger_suffixes = {"z",  # 640
                       "c",  # 800
                       "b",  # 1024
                       }

    path, ext = path.split(".")
    if path[-2] == "_":
        if path[-1] not in larger_suffixes:
            path = path[:-2]

    return url._replace(path=f"{path}.{ext}", scheme="http", params='', query='', fragment='')


# replace thumbnails, try to get full res images, skip proxies etc
def canonicalize_url(url):
    parsed = urlparse(url)
    hostname = parsed.netloc

    special_cases = {
        "flickr.com": canonicalize_flickr,
        "img.youtube.com": canonicalize_ytimg,
        "ytimg.com": canonicalize_ytimg,
        "wp.com": canonicalize_wp,
        "upload.wikimedia.org": canonicalize_wikimedia
    }

    for name, fn in special_cases.items():
        if name in hostname:
            return fn(parsed).geturl()

    return parsed.geturl()


def dump_url_from_file(fname, oname):
    try:
        f = gzip.open(fname, 'rb')
        fo = gzip.open(oname, 'wb', compresslevel=6)
        all_images = []

        for i in f:
            page_meta = []
            licenses = set()
            images = []
            try:
                p = json.loads(i)
            except:
                continue
            html_meta = p["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]
            links = html_meta["Links"]
            target_url = urlparse(p['Envelope']['WARC-Header-Metadata']['WARC-Target-URI'])
            target_path = target_url._replace(query="").geturl()

            try:
                if "Head" in html_meta:
                    if "Title" in html_meta["Head"]:
                        page_meta.append(html_meta["Head"]["Title"])
                    if "Metas" in html_meta["Head"]:
                        for m in html_meta["Head"]["Metas"]:
                            if "content" in m:
                                page_meta.append(m["content"])
            except:
                pass

            for img in links:
                try:
                    if len(images) > 100:
                        break
                    if "creativecommons" in img["url"]:
                        licenses.add(img["url"])
                    else:
                        if "alt" in img and len(img["alt"]) > 10 and img["url"].startswith("http"):
                            img["url"] = canonicalize_url(urljoin(target_path, img["url"]))
                            images.append(img)
                except:
                    pass

            for img in images:
                img["page_meta"] = set([img["alt"]] + page_meta)
                img["licenses"] = licenses
                img["alt"] = {img["alt"]}
                img["page_url"] = {target_path}
                all_images.append(img)

        deduped_images = {}
        for img in all_images:
            existing = deduped_images.get(img["url"], img)

            def get_or_update(key):
                e = existing.get(key, set())
                e.update(img[key])
                return e

            existing["alt"] = get_or_update("alt")
            existing["page_meta"] = get_or_update("page_meta")
            existing["licenses"] = get_or_update("licenses")
            existing["page_url"] = get_or_update("page_url")
            existing["count"] = existing.get("count", 0) + 1

            existing["page_meta"].difference_update(existing["alt"])

            deduped_images[img["url"]] = existing

        for img in deduped_images.values():
            img["alt"] = list(img["alt"])
            img["page_meta"] = list(img["page_meta"])
            img["licenses"] = list(img["licenses"])
            img["page_url"] = list(img["page_url"])

            h = hashlib.md5(img["url"].encode('utf-8')).hexdigest()
            img["hash"] = h
            fo.write(h.encode())
            fo.write(b" ")

            fo.write(json.dumps(img).encode())
            fo.write(b"\n")
        fo.close()

    except:
        print(f"\rfile {fname} failed to process")
        pass


def process(x):
    in_file, out_file = x
    Path("/".join(out_file.split("/")[:-1])).mkdir(parents=True, exist_ok=True)
    return dump_url_from_file(in_file, out_file)


if __name__ == "__main__":
    set_start_method("spawn")

    assert len(sys.argv) == 4

    input_dir = sys.argv[2]
    output_dir = sys.argv[3]
    thread = int(sys.argv[1])

    input_files = glob(f"{input_dir}/**/*")

    output_files = [i.replace(input_dir, output_dir, 1) for i in input_files]
    p = Pool(thread)

    urls = list(tqdm(p.imap_unordered(process, zip(input_files, output_files)), total=len(input_files)))
