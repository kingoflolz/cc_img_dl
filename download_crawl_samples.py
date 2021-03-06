# quick and dirty script to just download some records for testing
import pathlib
from multiprocessing import Pool
import random

import requests
from tqdm import tqdm

dir = ""
out = []

r = requests.get("https://the-eye.eu/eleuther_staging/comcrawl_output/current_tree.txt")

for i in r.iter_lines(decode_unicode=True):
    fname = i.strip().split(" ")[-1]

    if fname.startswith("CC-MAIN"):
        dir = fname
    else:
        out.append(f"https://the-eye.eu/eleuther_staging/comcrawl_output/{dir}/{fname}")

out = out[3:-3]
random.shuffle(out)


def download(url):
    dir = url.split("/")[-2]
    fname = url.split("/")[-1]

    pathlib.Path(f"./crawl/{dir}/").mkdir(parents=True, exist_ok=True)

    r = requests.get(url)
    open(f"./crawl/{dir}/{fname}", "wb+").write(r.content)


p = Pool(100)

list(tqdm(p.imap_unordered(download, out[:10000])))
