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
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, List

from jsonschema import validate

from getm import default_chunk_size, default_chunk_size_keep_alive
from getm.http import http
from getm.utils import indirect_open, resolve_target
from getm.progress import ProgressBar, ProgressLogger
from getm.reader import URLRawReader, URLReaderKeepAlive
from getm.checksum import Algorithms, GETMChecksum, part_count_from_s3_etag
from getm.concurrent.collections import ConcurrentHeap


logging.basicConfig()
logger = logging.getLogger(__name__)

class CLI:
    exit_code = 0
    continue_after_error = False
    cpu_count = multiprocessing.cpu_count()

    @classmethod
    def exit(cls, code: Optional[int]=None):
        sys.exit(code or cls.exit_code)

    @classmethod
    def log_debug(cls, **kwargs):
        logger.debug(json.dumps(kwargs))

    @classmethod
    def log_info(cls, **kwargs):
        logger.info(json.dumps(kwargs))

    @classmethod
    def log_warning(cls, **kwargs):
        logger.warning(json.dumps(kwargs))

    @classmethod
    def log_error(cls, **kwargs):
        cls.exit_code = 1
        logger.error(json.dumps(kwargs))

    @classmethod
    def log_exception(cls, **kwargs):
        cls.exit_code = 1
        logger.exception(json.dumps(kwargs))

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

def _multipart_buffer_size(concurrency: int) -> int:
    res = URLReaderKeepAlive.compute_buffer_size(concurrency, default_chunk_size_keep_alive)
    CLI.log_debug(multipart_buffer_size=res)
    return res

def _download(url: str,
              filepath: Optional[str],
              cs: Optional[GETMChecksum],
              concurrency: int,
              multipart_threshold: int):
    filepath = resolve_target(url, filepath)
    try:
        if multipart_threshold >= http.size(url):
            oneshot(url, filepath, cs)
        else:
            multipart(url, filepath, _multipart_buffer_size(concurrency), cs)
    except Exception:
        CLI.log_exception(message="Download failed!", url=url)
        raise

def download(manifest: List[dict], concurrency: int=CLI.cpu_count, multipart_threshold=default_chunk_size):
    assert 1 <= concurrency
    with ProcessPoolExecutor(max_workers=concurrency) as executor:
        cheap = ConcurrentHeap(executor, concurrency)
        for info in manifest:
            url = info['url']
            is_accessable, resp = http.accessable(url)
            if not is_accessable:
                assert resp is not None  # appease mypy
                CLI.log_error(message="url innaccessable", text=resp.text, url=url, status_code=resp.status_code)
                if not CLI.continue_after_error:
                    CLI.exit()
            else:
                if 'checksum' in info:
                    cs: Optional[GETMChecksum] = GETMChecksum(info['checksum'], info['checksum-algorithm'])
                else:
                    cs = None
                priority = -http.size(url)  # give small files larger priority
                cheap.priority_put(priority, _download, url, info.get('filepath'), cs, concurrency, multipart_threshold)
        try:
            for f in cheap.iter_futures():
                try:
                    f.result()
                except Exception:
                    if not CLI.continue_after_error:
                        CLI.exit()
        finally:
            # Attempt to halt subprocesses if parent dies prematurely
            cheap.abort()

def oneshot(url: str, filepath: str, cs: Optional[GETMChecksum]=None):
    cs = cs or checksum_for_url(url)
    log_info = dict(message="initiating oneshot download", url=url, expected_checksum=None)
    if cs:
        log_info['expected_checksum'] = cs.expected
        log_info['checksum_algorithm'] = cs.algorithm.name
    CLI.log_info(**log_info)
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
    CLI.log_debug(message="completed oneshot download", url=url)

def multipart(url: str, filepath: str, buffer_size: int, cs: Optional[GETMChecksum]=None):
    cs = cs or checksum_for_url(url)
    log_info = dict(message="initiating multipart download", url=url, expected_checksum=None)
    if cs:
        log_info['expected_checksum'] = cs.expected
        log_info['checksum_algorithm'] = cs.algorithm.name
    CLI.log_info(**log_info)
    with Progress.get(filepath, url) as progress:
        with indirect_open(filepath) as handle:
            for part in URLReaderKeepAlive.iter_content(url, default_chunk_size_keep_alive, buffer_size):
                handle.write(part)
                if cs:
                    cs.update(part)
                progress.add(len(part))
            if cs:
                assert cs.matches(), "Checksum failed!"
    CLI.log_debug(message="completed multipart download", url=url)

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
    CLI.log_debug(message="validating manifest", manifest=manifest, schema=manifest_schema)
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
    parser.add_argument("--concurrency",
                        type=int,
                        default=CLI.cpu_count,
                        help="Number of concurrent single part downloads")
    parser.add_argument("-v",
                        action="store_true",
                        help="Verbose mode")
    parser.add_argument("-vv",
                        action="store_true",
                        help="Very verbose mode")
    parser.add_argument("--multipart-threshold",
                        default=default_chunk_size,
                        help="multipart threshold")
    parser.add_argument("--continue-after-error",
                        "-c",
                        action="store_true",
                        help=("Continue downloading files if an error occurs."
                              "Exit status is non-zero if any downloads fail."))
    args = parser.parse_args(args=cli_args)
    if not (args.url or args.manifest) or (args.url and args.manifest):
        parser.print_usage()
        CLI.log_error(message="One of 'url' or '--manifest' must be specified, but not both.")
        CLI.exit()
    if not (1 <= args.concurrency):
        parser.print_usage()
        CLI.log_error(message="'--concurrency' must be '1' or larger.")
        CLI.exit()
    return args

def config_cli(args: argparse.Namespace):
    if args.vv:
        logger.setLevel(logging.DEBUG)
    elif args.v:
        logger.setLevel(logging.INFO)
    CLI.continue_after_error = args.continue_after_error

def main():
    """This is the main CLI entry point."""
    multiprocessing.set_start_method("fork")
    args = parse_args()
    config_cli(args)

    if args.url:
        info = dict(url=args.url)
        if args.filepath:
            info['filepath'] = args.filepath
        if args.checksum:
            info['checksum'] = args.checksum
        if args.checksum_algorithm:
            info['checksum-algorithm'] = args.checksum_algorithm
        manifest = [info]
        CLI.log_debug(progress_class=ProgressBar.__name__)
        Progress.progress_class = ProgressBar
    else:
        with open(args.manifest) as fh:
            manifest = json.loads(fh.read())
        CLI.log_debug(progress_class=ProgressLogger.__name__)
        Progress.progress_class = ProgressLogger

    _validate_manifest(manifest)
    download(manifest, args.concurrency, args.multipart_threshold)
    if CLI.exit_code:
        CLI.exit()
