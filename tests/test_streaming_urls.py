#!/usr/bin/env python
import io
import os
import sys
import time
import gzip
import boto3
import hashlib
import datetime
import requests
import warnings
import unittest
from math import ceil
from uuid import uuid4
from unittest import mock
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple

from google.cloud.storage import Client

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import streaming_urls as su
from streaming_urls.config import default_chunk_size, reader_retries


class GS:
    client = None
    bucket = None

class S3:
    bucket = None

def setUpModule():
    _suppress_warnings()
    if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        GS.client = Client.from_service_account_json(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    elif os.environ.get("STREAMING_URL_TEST_CREDENTIALS"):
        import json
        import base64
        from google.oauth2.service_account import Credentials
        creds_info = json.loads(base64.b64decode(os.environ.get("STREAMING_URL_TEST_CREDENTIALS")))
        creds = Credentials.from_service_account_info(creds_info)
        GS.client = Client(credentials=creds)
    else:
        GS.client = Client()
    GS.bucket = GS.client.bucket("gs-chunked-io-test")
    S3.bucket = boto3.resource("s3").Bucket("org-hpp-ssds-staging-test")

def tearDownModule():
    GS.client._http.close()

class TestStreamingURLsReader(unittest.TestCase):
    def setUp(self):
        _suppress_warnings()
        self.executor = ThreadPoolExecutor(max_workers=4)

    def tearDown(self):
        self.executor.shutdown()

    @classmethod
    def setUpClass(cls):
        cls.key = f"test_read/{uuid4()}"
        cls.data = os.urandom(1024)
        S3.bucket.Object(cls.key).upload_fileobj(io.BytesIO(cls.data))
        cls.s3_url = _generate_presigned_GET_url_s3(S3.bucket.name, cls.key)
        GS.bucket.blob(cls.key).upload_from_file(io.BytesIO(cls.data))
        cls.gs_url = _generate_presigned_GET_url_gs(GS.bucket.name, cls.key)

    def test_foo_seek(self):
        resp = requests.get(self.s3_url, stream=True)
        resp.raise_for_status()
        resp.raw.read(10)

    def duration_subtests(self, test_threads=[None, 4]):
        print()
        for threads in test_threads:
            start_time = time.time()
            try:
                yield threads
            except GeneratorExit:
                return
            print(self.id(), "duration", f"threads={threads}", time.time() - start_time)

    def test_reader_interface(self):
        with mock.patch("streaming_urls.reader._get_size"):
            reader = su.Reader("some_url")
            with self.assertRaises(OSError):
                reader.fileno()
            with self.assertRaises(OSError):
                reader.write(b"gadf")
            with self.assertRaises(OSError):
                reader.writelines(b"gadf")
            with self.assertRaises(OSError):
                reader.seek(123)
            with self.assertRaises(NotImplementedError):
                reader.tell()
            with self.assertRaises(NotImplementedError):
                reader.truncate()
            self.assertTrue(reader.readable())
            self.assertFalse(reader.isatty())
            self.assertFalse(reader.seekable())
            self.assertFalse(reader.writable())
            self.assertFalse(reader.closed)
            reader.close()
            self.assertTrue(reader.closed)

    def test_read(self):
        chunk_size = len(self.data) // 7
        for platform, url in [("aws", self.s3_url), ("gcp", self.gs_url)]:
            for threads in self.duration_subtests():
                with self.subTest(platform=platform, threads=threads):
                    with su.Reader(url, chunk_size, threads=threads) as reader:
                        data = reader.read()
                    self.assertEqual(self.data, data)

    def test_readinto(self):
        chunk_size = len(self.data) // 3
        buff = bytearray(2 * len(self.data))
        for threads in self.duration_subtests():
            with su.Reader(self.s3_url, chunk_size=chunk_size, threads=threads) as fh:
                bytes_read = fh.readinto(buff)
                self.assertEqual(self.data, buff[:bytes_read])

    def test_for_each_chunk(self):
        chunk_size = len(self.data) // 10
        for threads in self.duration_subtests([None, 2]):
            data = bytes()
            for chunk in su.for_each_chunk(self.gs_url, chunk_size=chunk_size, threads=threads):
                data += chunk
            self.assertEqual(data, self.data)

    def test_for_each_chunk_async(self):
        chunk_size = len(self.data) // 10
        number_of_chunks = ceil(len(self.data) / chunk_size)
        for threads in self.duration_subtests(test_threads=[1, 2]):
            chunks = [None] * number_of_chunks
            for chunk_number, chunk in su.for_each_chunk_async(self.s3_url, chunk_size=chunk_size, threads=threads):
                chunks[chunk_number] = chunk
            self.assertEqual(self.data, b"".join(chunks))

    @unittest.skip("FIXME: broken test")
    def test_fetch_chunk(self):
        url = mock.MagicMock()
        with mock.patch("streaming_urls.reader._download_chunk_from_url", return_value=b"SDFD") as mock_downloader:
            with mock.patch("streaming_urls.reader._get_size", return_value=20):
                reader = su.Reader(url)
                with self.assertRaises(ValueError):
                    reader._fetch_chunk(1)
                self.assertEqual(reader_retries, mock_downloader.call_count)

class TestBenchmark(unittest.TestCase):
    def duration_subtests(self, tests):
        print()
        for t in tests:
            start_time = time.time()
            try:
                with self.subTest(t[0]):
                    yield t
            except GeneratorExit:
                return
            print(self.id(), "duration", f"{t[0]}", time.time() - start_time)

    def test_decode_gzip_text(self):
        key, size = _put_gs_fixture()
        url = _generate_presigned_GET_url_gs(GS.bucket.name, key)
        tests = [(f"threads={threads}", threads) for threads in [None, 1, 2]]
        for test_name, threads in self.duration_subtests(tests):
            total = 0
            with su.Reader(url, threads=threads) as raw:
                with gzip.GzipFile(fileobj=raw) as gzip_reader:
                    with io.TextIOWrapper(gzip_reader, "ascii") as reader:
                        for line in reader:
                            total += len(line.encode("utf-8"))
            print(total)

    def test_for_each_chunk(self):
        key, size = _put_gs_fixture()
        url = _generate_presigned_GET_url_gs(GS.bucket.name, key)
        tests = [(f"threads={threads}", threads) for threads in [None, 1, 2]]
        for test_name, threads in self.duration_subtests(tests):
            for chunk in su.for_each_chunk(url, threads=threads):
                time.sleep(0.1)

    def test_for_each_chunk_async(self):
        key, size = _put_gs_fixture()
        url = _generate_presigned_GET_url_gs(GS.bucket.name, key)
        tests = [
            ("threads=1", 1),
            ("threads=2", 2),
            ("threads=3", 3),
        ]
        tests = [(f"threads={threads}", threads) for threads in range(1,4)]
        for test_name, threads in self.duration_subtests(tests):
            for i, chunk in su.for_each_chunk_async(url, threads=threads):
                time.sleep(0.1)

def _generate_presigned_GET_url_s3(bucket: str, key: str) -> str:
    args = dict(Bucket=bucket, Key=key)
    url = boto3.client("s3").generate_presigned_url(ClientMethod="get_object", Params=args)
    return url

def _generate_presigned_GET_url_gs(bucket: str, key: str) -> str:
    blob = GS.bucket.blob(key)
    url = GS.client.bucket(bucket).blob(key).generate_signed_url(datetime.timedelta(days=1),
                                                                 version="v4")
    return url

def _put_gs_fixture() -> Tuple[str, int]:
    key = "streaming-urls-large-file"
    blob = GS.bucket.get_blob(key)
    if not blob:
        data = _random_gzipped_text(1024 * 1024 * 200)
        GS.bucket.blob(key).upload_from_file(io.BytesIO(data))
    return key, GS.bucket.get_blob(key).size

def _random_gzipped_text(size=1024 * 1024 * 200) -> bytes:
    import numpy as np
    import gzip
    foo = np.random.randint(48, 122, size=size, dtype=np.uint8)
    reader = io.BytesIO(foo.tobytes())
    writer = io.BytesIO()
    linesep = os.linesep.encode("ascii")
    with gzip.GzipFile(fileobj=writer, mode="w") as gzip_writer:
        while True:
            d = reader.read(80)
            if not d:
                break
            gzip_writer.write(d + linesep)
    return writer.getvalue()

def _suppress_warnings():
    # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
    # Suppress unclosed socket warnings
    warnings.simplefilter("ignore", ResourceWarning)

if __name__ == '__main__':
    unittest.main()
