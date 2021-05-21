#!/usr/bin/env python
"""Upload random data to Google Storage. Size can be larger than the amount of RAM available on your system.
The is mostly for testing getm but if you want to do something weird go ahead.
"""
import os
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor

from google.cloud.storage import Client
from gs_chunked_io.writer import AsyncPartUploader
from gs_chunked_io.async_collections import AsyncSet


parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--key",
                    "-k",
                    help="Google Storage object key",
                    default="random-data")
parser.add_argument("--bucket",
                    "-b",
                    help="Google Storage bucket name. Default to value of env var 'GETM_GS_TEST_BUCKET'",
                    type=str, default=os.environ.get('GETM_GS_TEST_BUCKET'))
parser.add_argument("--size",
                    type=int,
                    default=1024 ** 3 * 25,
                    help="size in bytes")
parser.add_argument("--chunk-size",
                    type=int,
                    help="size of chunks of data uploaded to GS",
                    default=1024 ** 2 * 32)
args = parser.parse_args()
if not args.bucket:
    print(parser.print_help())
    sys.exit(1)

bucket = Client().bucket(args.bucket)

num_parts = args.size // args.chunk_size
concurrency = 4

with ThreadPoolExecutor(max_workers=concurrency) as e:
    with AsyncPartUploader(args.key, bucket, AsyncSet(e, concurrency)) as uploader:
        for i in range(num_parts -1, -1, -1):
            uploader.put_part(i, os.urandom(args.chunk_size))
            print("Uploading part", 1 + i)
