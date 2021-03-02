import gzip
import json
from urllib.parse import urlparse, urljoin


def dump_url_from_file(fname, oname):
    f = gzip.open(fname, 'rb')
    fo = gzip.open(oname, 'wb')
    all_images = []

    for i in f:
        page_meta = []
        licenses = set()
        images = []
        p = json.loads(i)
        html_meta = p["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]
        links = html_meta["Links"]
        target_url = urlparse(p['Envelope']['WARC-Header-Metadata']['WARC-Target-URI'])
        target_path = target_url._replace(query="").geturl()

        if "Head" in html_meta:
            if "Title" in html_meta["Head"]:
                page_meta.append(html_meta["Head"]["Title"])
            if "Metas" in html_meta["Head"]:
                for m in html_meta["Head"]["Metas"]:
                    if "content" in m:
                        page_meta.append(m["content"])

        for img in links:
            if "creativecommons" in img["url"]:
                licenses.add(img["url"])
            else:
                if "ytimg" in img["url"]:
                    print()

                if "alt" in img and len(img["alt"]) > 10:
                    img["url"] = urljoin(target_path, img["url"])
                    images.append(img)

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


dump_url_from_file("example.jsonl.wat.gz", "example_out.jsonl.gz")