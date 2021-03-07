import subprocess
import multiprocessing
from multiprocessing import set_start_method
from functools import partial
from tqdm import tqdm

import sys
import pathlib


def process_wat(url, output_path):
    if not url.strip():
        return url

    output_name = url.split("/")[3] + "_" + url.split("/")[-1].replace(".warc.wat.gz", ".jsonl.wat.gz")
    dir_name = url.split("/")[1]

    pathlib.Path(f"{output_path}/{dir_name}/").mkdir(parents=True, exist_ok=True)

    while True:
        try:
            subprocess.run(["./commoncrawl_filter", "http://commoncrawl.s3.amazonaws.com/" + url, f"{output_path}/{dir_name}/{output_name}".strip()], timeout=1200, check=True)
            break
        except:
            pass

    return url


if __name__=="__main__":
    set_start_method("spawn")

    assert len(sys.argv) == 4

    f = open(sys.argv[2])
    total = len(f.readlines())
    f.seek(0)

    p = multiprocessing.Pool(int(sys.argv[1]))
    process = partial(process_wat, output_path=sys.argv[3])

    with open('progress.txt', 'a+') as o_f:
        for i in tqdm(p.imap(process, f), total=total):
            o_f.write(i)
            o_f.flush()
