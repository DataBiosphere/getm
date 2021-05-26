"""Download data from http(s) URLs to the local file system, verifying integrity."""
import os
import sys
import json
import pprint
import logging
import warnings
import argparse
import multiprocessing
from math import ceil
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional, List

from jsonschema import validate

from getm import default_chunk_size, default_chunk_size_keep_alive
from getm.http import http
from getm.utils import indirect_open, resolve_target
from getm.progress import ProgressBar, ProgressLogger
from getm.reader import URLRawReader, URLReaderKeepAlive
from getm.checksum import Algorithms, GETMChecksum, part_count_from_s3_etag


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def checksum_for_url(url: str) -> Optional[GETMChecksum]:
    """Probe headers for checksum information, return or None."""
    cs: Optional[GETMChecksum]
    checksums = http.checksums(url)
    if 'gs_crc32c' in checksums:
        cs = GETMChecksum(checksums['gs_crc32c'], "gs_crc32c")
    elif 's3_etag' in checksums:
        cs = GETMChecksum(checksums['s3_etag'], "s3_etag")
        cs.set_s3_size_and_part_count(http.size(url), part_count_from_s3_etag(checksums['s3_etag']))
    elif 'md5' in checksums:
        cs = GETMChecksum(checksums['md5'], "md5")
    else:
        warnings.warn(f"No checksum information for '{url}'")
        cs = None
    return cs

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
    multipart_buffer_size = URLReaderKeepAlive.compute_buffer_size(multipart_concurrency, default_chunk_size_keep_alive)
    logger.debug(f"multipart buffer size: {multipart_buffer_size}")
    with ProcessPoolExecutor(max_workers=oneshot_concurrency) as oneshot_executor:
        with ProcessPoolExecutor(max_workers=multipart_concurrency) as multipart_executor:
            futures = dict()
            for info in manifest:
                url = info['url']
                if 'checksum' in info:
                    cs: Optional[GETMChecksum] = GETMChecksum(info['checksum'], info['checksum-algorithm'])
                else:
                    cs = None
                filepath = resolve_target(url, info.get('filepath'))
                if multipart_threshold >= http.size(url):
                    f = oneshot_executor.submit(oneshot, url, filepath, cs)
                else:
                    f = multipart_executor.submit(multipart, url, filepath, multipart_buffer_size, cs)
                futures[f] = url
            try:
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception:
                        logger.exception(f"Failed to download '{futures[f]}'")
            finally:
                # Attempt to halt subprocesses if parent dies prematurely
                for f in futures:
                    if not f.done():
                        f.cancel()

def oneshot(url: str, filepath: str, cs: Optional[GETMChecksum]=None):
    cs = cs or checksum_for_url(url)
    with URLRawReader(url) as handle:
        data = handle.read()
        try:
            if cs:
                cs.update(data)
                assert cs.matches(), "Checksum failed!"
            with open(filepath, "wb", buffering=0) as fh:
                fh.write(data)
            with Progress.get(filepath, url) as progress:
                progress.add(len(data))
        finally:
            data.release()

def multipart(url: str, filepath: str, buffer_size: int, cs: Optional[GETMChecksum]=None):
    cs = cs or checksum_for_url(url)
    with Progress.get(filepath, url) as progress:
        with indirect_open(filepath) as handle:
            for part in URLReaderKeepAlive.iter_content(url, default_chunk_size_keep_alive, buffer_size):
                handle.write(part)
                if cs:
                    cs.update(part)
                progress.add(len(part))
            if cs:
                assert cs.matches(), "Checksum failed!"

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
                        help=(f"Algorithm to compute checksum.{os.linesep}"
                              f"Must be one of {[a.name for a in Algorithms]}"))
    parser.add_argument("--manifest",
                        "-m",
                        "-i",
                        help=manifest_arg_help)
    parser.add_argument("--oneshot-concurrency",
                        default=4,
                        help="Number of concurrent single part downloads")
    parser.add_argument("--multipart-concurrency",
                        default=2,
                        help="Number of concurrent multipart downloads. Can either be '1' or '2'")
    parser.add_argument("--multipart-threshold",
                        default=default_chunk_size,
                        help="multipart threshold")
    args = parser.parse_args(args=cli_args)
    if not (args.url or args.manifest) or (args.url and args.manifest):
        parser.print_usage()
        print()
        print("One of 'url' or '--manifest' must be specified, but not both.")
        sys.exit(1)
    if not (1 <= args.multipart_concurrency <= 2):
        parser.print_usage()
        sys.exit(1)
    return args

def main():
    """This is the main CLI entry point."""
    multiprocessing.set_start_method("fork")
    args = parse_args()

    if args.url:
        info = dict(url=args.url)
        if args.filepath:
            info['filepath'] = args.filepath
        if args.checksum:
            info['checksum'] = args.checksum
        if args.checksum_algorithm:
            info['checksum-algorithm'] = args.checksum_algorithm
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
