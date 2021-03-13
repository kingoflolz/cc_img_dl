# Attempt decompression, compute metadata and (maybe) resize/reencode image
import gzip
import json
import os
import shutil
import sys
from functools import partial
from glob import glob
from multiprocessing import Pool, set_start_method, Process
from pathlib import Path
import random

import cv2
import filetype
from tqdm import tqdm


def jsonl_generator(fname):
    f = gzip.open(fname, 'rb')
    for i in f:
        try:
            yield json.loads(i)
        except:
            pass


def convert_file(input_file, output_file):
    kind = filetype.guess(input_file)

    jpeg = kind.mime == "image/jpeg"
    cv2_readable = kind.mime == "image/jpeg" or \
                   kind.mime == "image/png" or \
                   kind.mime == "image/webp" or \
                   kind.mime == "image/bmp" or \
                   kind.mime == "image/tiff" or \
                   kind.mime == "image/gif"

    if cv2_readable:
        img = cv2.imread(input_file)
        size = img.shape[:2]
        pixels = size[0] * size[1]
        aspect_ratio = max(size) / min(size)

        valid = pixels > 5000 and aspect_ratio < 2
        too_large = max(size) > 2048

        if not valid:
            return {}

        encode_param = [int(cv2.IMWRITE_JPEG_QUALITY), 90]

        if not too_large and jpeg:
            # just copy it to avoid encoding again
            shutil.copyfile(input_file, output_file)
            return {"orig_dim": size, "new_dim": size}

        elif not too_large:
            # reencode as jpeg
            success, buffer = cv2.imencode(".jpg", img, encode_param)
            if not success:
                return {}

            buffer.tofile(output_file)
            return {"orig_dim": size, "new_dim": size}

        elif too_large:
            # resize and then encode as jpeg
            ratio = 2048 / max(size)
            width = round(size[1] * ratio)
            height = round(size[0] * ratio)

            img = cv2.resize(img, (width, height), interpolation=cv2.INTER_AREA)
            success, buffer = cv2.imencode(".jpg", img, encode_param)
            if not success:
                return {}

            buffer.tofile(output_file)
            return {"orig_dim": size, "new_dim": img.shape[:2]}
    else:
        return {}


def process_jsonl(file, img_in_root_dir, label_in_root_dir, img_out_root_dir, label_out_root_dir):
    sys.stdout = open(os.devnull, "w")
    sys.stderr = open(os.devnull, "w")

    input_dir = "/".join(file.split("/")[:-1])
    leaf_dir = input_dir.replace(label_in_root_dir, "")
    Path(img_out_root_dir + leaf_dir).mkdir(parents=True, exist_ok=True)
    Path(label_out_root_dir + leaf_dir).mkdir(parents=True, exist_ok=True)

    f = gzip.open(label_out_root_dir + leaf_dir + "/" + file.split("/")[-1], 'wb')

    for record in jsonl_generator(file):
        try:
            image_in = img_in_root_dir + leaf_dir + "/" + record['hash']
            image_out = img_out_root_dir + leaf_dir + "/" + record['hash']

            additional_meta = convert_file(image_in, image_out)

            if additional_meta:
                f.write(json.dumps({
                    **record,
                    **additional_meta
                }).encode())
                f.write(b"\n")
        except:
            pass

    f.close()


if __name__ == "__main__":
    set_start_method("spawn")

    assert len(sys.argv) == 6

    threads = int(sys.argv[1])
    image_in_dir = sys.argv[2]
    label_in_dir = sys.argv[3]
    image_out_dir = sys.argv[4]
    label_out_dir = sys.argv[5]

    p = Pool(threads)

    input_files = glob(f"{label_in_dir}/*/*/*")
    random.shuffle(input_files)

    process = partial(process_jsonl,
                      img_in_root_dir=image_in_dir,
                      label_in_root_dir=label_in_dir,
                      img_out_root_dir=image_out_dir,
                      label_out_root_dir=label_out_dir)

    list(tqdm(p.imap_unordered(process, input_files), total=len(input_files), desc="Convert images", file=sys.stdout))

    # process_jsonl("deduped_urls/0/0/deduped.jsonl.gz", "images", "deduped_urls", "converted_images", "labels")
