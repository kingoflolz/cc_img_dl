# Runs img_dl program across all deduplicated url jsonls
import subprocess
from glob import glob
import sys
from multiprocessing import Pool, set_start_method
from pathlib import Path
import random

from tqdm import tqdm


def process_download(input_file, input_root_dir, output_root_dir, error_root_dir):
    input_file_seg = input_file.replace(input_root_dir, "", count=1).split("/")
    input_dir = "/".join(input_file_seg[-1])

    error_dir = input_dir.replace(input_root_dir, error_root_dir, count=1)
    Path(error_dir).mkdir(parents=True, exist_ok=True)
    error_filename = error_dir + "/errors.jsonl.gz"

    out_dir = input_dir.replace(input_root_dir, output_root_dir, count=1)
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    while True:
        try:
            subprocess.run(["./img_dl_bin", input_file, out_dir, error_filename], check=True)
            break
        except:
            pass


if __name__ == "__main__":
    set_start_method("spawn")

    assert len(sys.argv) == 5

    threads = int(sys.argv[1])
    input_dir = sys.argv[2]
    errors_dir = sys.argv[3]
    out_dir = sys.argv[4]

    p = Pool(threads)

    input_files = glob(f"{input_dir}/**/*")
    random.shuffle(input_files)

    list(tqdm(p.imap_unordered(process_download, input_files), total=len(input_files), desc="Download images"))

