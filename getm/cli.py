"""Download data from http(s) URLs to the local file system, verifying integrity."""
import os
import sys
import json
import pprint
import logging
import argparse
from math import ceil
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, List

from jsonschema import validate

from getm import urlopen, iter_content, default_chunk_size
from getm.http import http
from getm.checksum import Algorithms
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

def download(manifest: List[dict],
             oneshot_concurrency: int=4,
             multipart_concurrency: int=2,
             multipart_threshold=default_chunk_size):
    assert 1 <= oneshot_concurrency
    assert 1 <= multipart_concurrency
    with ProcessPoolExecutor(max_workers=oneshot_concurrency) as oneshot_executor:
        with ProcessPoolExecutor(max_workers=multipart_concurrency) as multipart_executor:
            for info in manifest:
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
            if cs:
                assert cs.matches(expected_cs), "Checksum failed!"

# TODO: validate URL format
manifest_schema = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "url": {"type": "string"},
            "filepath": {"type": "string"},
            "checksum": {"type": "string"},
            "checksum-algorithm": {"type": "string", "enum": [e.name for e in Algorithms]},
        },
        "required": ["url"],
        "dependencies": {
            "checksum": ["checksum-algorithm"],
            "checksum-algorithm": ["checksum"],
        }
    },
}

def _validate_manifest(manifest: dict):
    validate(instance=manifest, schema=manifest_schema)

manifest_arg_help = f"""Download URls as specified in a local json FILE
FILE must conform to the following schema

{pprint.pformat(manifest_schema, width=72)}

If 'checksum' and 'checksum-algorithm' is omitted, warnings will be issued if
checksum information is not contained in headers.  if 'checksum-algorithm' is
'null' warnings are omitted.
"""

class HelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    def _split_lines(self, text, width):
        return text.splitlines()

def parse_args(cli_args: Optional[List[str]]=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=HelpFormatter)
    parser.add_argument("url",
                        nargs="?",
                        metavar="url",
                        type=str,
                        default=None,
                        help="URL to download. This argument has greatest precedence.")
    parser.add_argument("--filepath",
                        "-O",
                        help="Local file path")
    parser.add_argument("--checksum",
                        "-cs",
                        help="Expected checksum value")
    parser.add_argument("--checksum-algorithm",
                        "-ca",
                        default="null",
                        help=(f"Algorithm to compute checksum.{os.linesep}"
                              "Must be one of {[a.name for a in Algorithms]}"))
    parser.add_argument("--manifest",
                        "-m",
                        "-i",
                        help=manifest_arg_help)
    parser.add_argument("--oneshot-concurrency",
                        default=4,
                        help="Number of concurrent single part downloads")
    parser.add_argument("--multipart-concurrency",
                        default=2,
                        help="Number of concurrent multipart downloads")
    parser.add_argument("--multipart-threshold",
                        default=default_chunk_size,
                        help="multipart threshold")
    args = parser.parse_args(args=cli_args)
    return args

def main():
    """This is the main CLI entry point."""
    args = parse_args()

    if args.url:
        info = dict(url=args.url)
        if args.filepath:
            info['filepath'] = args.filepath
        if args.checksum:
            info['checksum'] = args.checksum
        if args.checksum_algorithm:
            info['checksum_algorithm'] = args.checksum_algorithm
        manifest = [info]
    else:
        with open(args.manifest) as fh:
            manifest = json.loads(fh.read())

    _validate_manifest(manifest)

    if 1 == len(manifest):
        Progress.progress_class = ProgressBar
    else:
        Progress.progress_class = ProgressLogger

    download(manifest, args.oneshot_concurrency, args.multipart_concurrency, args.multipart_threshold)
