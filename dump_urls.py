import gzip
import json
import sys
from glob import glob
from multiprocessing import Pool, set_start_method
from pathlib import Path
from urllib.parse import urlparse, urljoin

from tqdm import tqdm


def dump_url_from_file(fname, oname):
    try:
        f = gzip.open(fname, 'rb')
        fo = gzip.open(oname, 'wb')
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
                    if "creativecommons" in img["url"]:
                        licenses.add(img["url"])
                    else:
                        if "alt" in img and len(img["alt"]) > 10:
                            img["url"] = urljoin(target_path, img["url"])
                            images.append(img)
                except:
                    pass

            for img in images:
                img["page_meta"] = set([img["alt"]] + page_meta)
                img["licenses"] = licenses
                img["alt"] = set([img["alt"]])
                img["count"] = 1
                all_images.append(img)

        deduped_images = {}
        for img in all_images:
            existing = deduped_images.get(img["url"], {})

            def get_or_update(key):
                e = existing.get(key, set())
                e.update(img[key])
                return e

            existing["alt"] = get_or_update("alt")
            existing["page_meta"] = get_or_update("page_meta")
            existing["licenses"] = get_or_update("licenses")
            existing["count"] = existing.get("count", 0) + 1

            existing["page_meta"].difference_update(existing["alt"])

            deduped_images[img["url"]] = existing

        for img in deduped_images.values():
            img["alt"] = list(img["alt"])
            img["page_meta"] = list(img["page_meta"])
            img["licenses"] = list(img["licenses"])
            fo.write(json.dumps(img).encode())
            fo.write(b"\n")
        fo.close()

    except:
        print(f"file {input_files} failed to process")
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