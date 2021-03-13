# use a parallel radix sort of url hashes to deduplicate urls
import itertools
import json
from glob import glob
import gzip
import sys
from itertools import product
from multiprocessing import Pool, set_start_method
from pathlib import Path
import random

from tqdm import tqdm


def parse_jsonl(fname):
    f = gzip.open(fname, 'rb')
    records = []
    try:
        for i in f:
            records.append(json.loads(i))
    except:
        pass
    return records


def read_with_hash(fname):
    try:
        f = gzip.open(fname, 'rb')
        records = []
        for i in f:
            h, r = i.split(b" ", maxsplit=1)

            records.append((h, r))
        return records
    except:
        return []


def get_dirs(out_dir, out_levels):
    hex_characters = list("0123456789abcdef")
    directories = list(product(hex_characters, repeat=out_levels))
    out = []
    for d in directories:
        out.append(Path(f"{out_dir}/{'/'.join(d)}"))
    return out


def scatter_files(flist, thread_index, out_dir, out_levels):
    dirs = get_dirs(out_dir, out_levels)
    files = [i/f"scatter_{thread_index}.jsonl.gz" for i in dirs]
    [Path(i).mkdir(parents=True, exist_ok=True) for i in dirs]
    files = [gzip.open(i, "wb", compresslevel=6) for i in files]

    for i in flist:
        parsed = read_with_hash(i)
        for h, r in parsed:
            hash_prefix = h[:out_levels]
            file = files[int(hash_prefix, 16)]
            file.write(r)

    [i.close() for i in files]


def scatter_process(x):
    (idx, file_chunks), out_dir, out_levels = x
    scatter_files(file_chunks, idx, out_dir, out_levels)


def dedup(input_dir, out_file):
    images = {}

    for f in glob(f"{input_dir}/*"):
        records = parse_jsonl(f)

        for i in records:
            if i["hash"] in images:
                new = images[i["hash"]]

                new["alt"].update(i["alt"])
                new["page_meta"].update(i["page_meta"])
                new["licenses"].update(i["licenses"])
                new["page_url"].update(i["page_url"])
            else:
                i["alt"] = set(i["alt"])
                i["page_meta"] = set(i["page_meta"])
                i["licenses"] = set(i["licenses"])
                i["page_url"] = set(i["page_url"])

                new = i

            images[i["hash"]] = new

    of = gzip.open(out_file, "w", compresslevel=6)
    for i in images.values():
        i["alt"] = list(i["alt"])
        i["page_meta"] = list(i["page_meta"])
        i["page_url"] = list(i["page_url"])
        i["licenses"] = list(i["licenses"])

        of.write(json.dumps(i).encode())
        of.write(b"\n")

    of.close()


def dedup_process(x):
    input_dir, out_file = x
    Path("/".join(out_file.split("/")[:-1])).mkdir(parents=True, exist_ok=True)
    dedup(input_dir, out_file)


if __name__ == "__main__":
    set_start_method("spawn")

    assert len(sys.argv) == 5

    input_dir = sys.argv[2]
    cluster_dir = sys.argv[3]
    out_dir = sys.argv[4]
    threads = int(sys.argv[1])

    p = Pool(threads)

    input_files = glob(f"{input_dir}/**/*")
    random.shuffle(input_files)

    output_chunks = threads * 4

    chunked_input = list(enumerate([input_files[i::output_chunks] for i in range(output_chunks)]))

    list(tqdm(p.imap_unordered(scatter_process,
                               zip(chunked_input,
                                   itertools.cycle([cluster_dir]),  # infinite iterators
                                   itertools.cycle([2]))
                               ), total=len(chunked_input), desc="hash scatter stage"))

    shard_dirs = glob(f"{cluster_dir}/*/*/")

    [Path(i.replace(cluster_dir, out_dir, 1)).mkdir(parents=True, exist_ok=True) for i in shard_dirs]

    output_files = [i.replace(cluster_dir, out_dir, 1) + "deduped.jsonl.gz" for i in shard_dirs]

    list(tqdm(p.imap_unordered(dedup_process,
                               zip(shard_dirs, output_files)),
              total=len(shard_dirs), desc="gather dedup stage"))

