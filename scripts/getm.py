#!/usr/bin/env python
"""
Given a GS signed URL and filepath, download data to filepath, verify md5 checksum.
"""
# gs://gs-chunked-io-test/getm-large-file
import os
import sys
import time

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from getm import reader, default_chunk_size, download_iter_parts


url = sys.argv[1]
filepath = os.path.realpath(sys.argv[2])
sz = reader.http.size(url)

start = time.time()
print("[", end="", flush=True)
part_count = 0
total = 0
for part_size in download_iter_parts(url, filepath):
    total += part_size
    new_part_count = total // default_chunk_size
    if new_part_count > part_count:
        print("=", end="", flush=True)
        part_count = new_part_count
print("]")
print()
print(f"download {sz} bytes in {time.time() - start} seconds")
