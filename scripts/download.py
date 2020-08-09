#!/usr/bin/env python
"""
Given a GS signed URL and filepath, download data to filepath, verify md5 checksum.
"""
# gs://gs-chunked-io-test/streaming-urls-large-file
import os
import sys
import time
import base64
import hashlib

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from streaming_urls.reader import for_each_part, default_chunk_size, http


def get_md5_from_gs_signed_url(url: str):
    headers = http.head(url)
    gs_hashes = headers['x-goog-hash']
    md5 = gs_hashes.split(",")[1].split("=", 1)[1]
    return md5

url = sys.argv[1]
expected_md5 = get_md5_from_gs_signed_url(url)
filepath = os.path.realpath(sys.argv[2])
sz = http.size(url)
try:
    print("[", end="", flush=True)
    start = time.time()
    fd = os.open(filepath, os.O_WRONLY | os.O_CREAT)
    os.pwrite(fd, b"0", sz - 1)
    md5 = hashlib.md5()
    for chunk_id, chunk in enumerate(for_each_part(url, concurrency=4)):
        md5.update(chunk)
        os.pwrite(fd, chunk, chunk_id * default_chunk_size)
        chunk.release()
        print("=", end="", flush=True)
except Exception:
    os.remove(filepath)
    sys.exit(1)
finally:
    os.close(fd)
print("]")
print()
assert expected_md5 == base64.b64encode(md5.digest()).decode("utf-8"), "md5 checksum failed!"
print(f"download {sz} bytes in {time.time() - start} seconds")
