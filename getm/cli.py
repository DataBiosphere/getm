"""Download data from http(s) URLs to the local file system, verifying integrity."""
import sys
import json
import logging
import argparse
from math import ceil
from concurrent.futures import ProcessPoolExecutor
from typing import List

from getm import urlopen, iter_content, default_chunk_size
from getm.http import http
from getm.utils import checksum_for_url, indirect_open
from getm.progress import ProgressBar, ProgressLogger


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

class Progress:
    progress_class: type = ProgressBar

    @classmethod
    def get(cls, name: str, url: str):
        sz = http.size(url)
        if cls.progress_class == ProgressBar:
            incriments = 40
        else:
            incriments = ceil(sz / default_chunk_size / 2)
        return cls.progress_class(name, sz, incriments)

def download(url_info: List[dict],
             oneshot_concurrency: int=4,
             multipart_concurrency: int=2,
             multipart_threshold=default_chunk_size):
    assert 1 <= oneshot_concurrency
    assert 1 <= multipart_concurrency
    with ProcessPoolExecutor(max_workers=oneshot_concurrency) as oneshot_executor:
        with ProcessPoolExecutor(max_workers=multipart_concurrency) as multipart_executor:
            for info in url_info:
                url = info['url']
                filepath = info.get('filepath') or http.name(url)
                if multipart_threshold >= http.size(url):
                    oneshot_executor.submit(oneshot, url, filepath)
                else:
                    multipart_executor.submit(multipart, url, filepath)

def oneshot(url: str, filepath: str):
    expected_cs, cs = checksum_for_url(url)
    assert expected_cs and cs, "No checksum information available!"
    with urlopen(url, concurrency=None) as handle:
        data = handle.read()
        try:
            cs.update(data)
            assert cs.matches(expected_cs), "Checksum failed!"
            with open(filepath, "wb", buffering=0) as fh:
                fh.write(data)
            with Progress.get(filepath, url) as progress:
                progress.add(len(data))
        finally:
            data.release()

def multipart(url: str, filepath: str):
    expected_cs, cs = checksum_for_url(url)
    assert expected_cs and cs, "No checksum information available!"
    with Progress.get(filepath, url) as progress:
        with indirect_open(filepath) as handle:
            for part in iter_content(url, concurrency=1):
                handle.write(part)
                cs.update(part)
                progress.add(len(part))
            assert cs.matches(expected_cs), "Checksum failed!"

manifest_arg_doc = """Download URls as specified in a local json FILE
The json is expected to be an array of elements of the following structure. The only required field is 'url'.
{
  'url':
  'filepath': # local name on file system
}
"""

def main():
    """This is the main entry point for the CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("url",
                        nargs="?",
                        metavar="url",
                        type=str,
                        default=None,
                        help="URL to download. This argument has greatest precedence.")
    parser.add_argument("--filepath", "-O", help="Local file path")
    parser.add_argument("--manifest", "-m", "-i", help=manifest_arg_doc)
    parser.add_argument("--oneshot-concurrency", default=4)
    parser.add_argument("--multipart-concurrency", default=2)
    parser.add_argument("--multipart-threshold", default=default_chunk_size)
    args = parser.parse_args()

    if not (args.url or args.manifest):
        parser.print_usage()
        sys.exit(1)

    if args.url:
        info = dict(url=args.url)
        if args.filepath:
            info['filepath'] = args.filepath
        url_info = [info]
    else:
        with open(args.manifest) as fh:
            url_info = json.loads(fh.read())

    if 1 == len(url_info):
        Progress.progress_class = ProgressBar
    else:
        Progress.progress_class = ProgressLogger

    download(url_info, args.oneshot_concurrency, args.multipart_concurrency, args.multipart_threshold)
