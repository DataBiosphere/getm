#!/usr/bin/env python
import io
import os
import sys
import time
import unittest
import contextlib
from math import ceil
from uuid import uuid4
from unittest import mock
from random import randint

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import streaming_urls
from tests.infra import GS, S3, suppress_warnings


def setUpModule():
    suppress_warnings()
    GS.setup()
    S3.setup()

def tearDownModule():
    GS.client._http.close()

class TestStreamingURLsReader(unittest.TestCase):
    def setUp(self):
        suppress_warnings()

    @classmethod
    def setUpClass(cls):
        cls.key = f"test_read/{uuid4()}"
        cls.expected_data = os.urandom(1024)
        S3.bucket.Object(cls.key).upload_fileobj(io.BytesIO(cls.expected_data))
        cls.s3_url = S3.generate_presigned_GET_url(cls.key)
        GS.bucket.blob(cls.key).upload_from_file(io.BytesIO(cls.expected_data))
        cls.gs_url = GS.generate_presigned_GET_url(cls.key)

    def test_reader_interface(self):
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch("streaming_urls.http.Session.size"))
            stack.enter_context(mock.patch("streaming_urls.reader.SharedCircularBuffer"))
            stack.enter_context(mock.patch("streaming_urls.reader.ProcessPoolExecutor"))
            stack.enter_context(mock.patch("streaming_urls.reader.ConcurrentQueue"))
            for concurrency in [None, 4]:
                with streaming_urls.urlopen("some_url") as reader:
                    with self.assertRaises(OSError):
                        reader.fileno()
                    with self.assertRaises(OSError):
                        reader.write(b"x")
                    with self.assertRaises(OSError):
                        reader.writelines(b"x")
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
                self.assertTrue(reader.closed)

    def test_read(self):
        for platform, url in [("aws", self.s3_url), ("gcp", self.gs_url)]:
            for concurrency in (None, 3):
                with self.subTest(platform=platform, concurrency=concurrency):
                    self._test_read(url, concurrency)

    def _test_read(self, url: str, concurrency: int, chunk_size=None):
        chunk_size = chunk_size or len(self.expected_data) // 5
        with streaming_urls.urlopen(url, chunk_size, concurrency=concurrency) as reader:
            data = bytearray()
            while True:
                d = reader.read(randint(chunk_size // 3, chunk_size))
                if not d:
                    break
                data += d
                d.release()
            self.assertEqual(self.expected_data, data)

    def test_read_no_overlap(self):
        """
        Ensure it is not possible to overlap the circular buffer.
        """
        chunk_size = len(self.expected_data) // 5
        with streaming_urls.urlopen(self.gs_url, chunk_size, concurrency=2) as reader:
            view = reader.read(1)
            expected_first_byte = bytes(view)
            try:
                reader.read(reader.max_read).release()
                # wait for part downloads to complete
                for _ in range(20):
                    if not reader.future_parts.running():
                        break
                    time.sleep(0.1)
                else:
                    self.fail("Waited to long for http response subprocesses.")
                first_byte = bytes(view)
                self.assertEqual(expected_first_byte, first_byte)
            finally:
                view.release()

    def test_readinto(self):
        chunk_size = len(self.expected_data) // 3
        for platform, url in [("aws", self.s3_url), ("gcp", self.gs_url)]:
            for concurrency in (None, 3, 5):
                with self.subTest(platform=platform, concurrency=concurrency):
                    self._test_readinto(url, concurrency, chunk_size)

    def _test_readinto(self, url: str, concurrency: int, chunk_size: int):
        buff = bytearray(2 * len(self.expected_data))
        with streaming_urls.urlopen(self.s3_url, chunk_size=chunk_size, concurrency=concurrency) as fh:
            bytes_read = fh.readinto(buff)
            self.assertEqual(self.expected_data[:bytes_read], buff[:bytes_read])

    def test_iter_content(self):
        chunk_size = len(self.expected_data) // 10
        for concurrency in (None, 2):
            data = bytearray()
            for chunk in streaming_urls.iter_content(self.gs_url, chunk_size=chunk_size, concurrency=concurrency):
                data += chunk
                chunk.release()
            self.assertEqual(data, self.expected_data)

    def test_iter_content_unordered(self):
        chunk_size = len(self.expected_data) // 50
        number_of_chunks = ceil(len(self.expected_data) / chunk_size)

        for concurrency in (8, 10):
            data = bytearray(len(self.expected_data))
            for chunk_id, chunk in streaming_urls.iter_content_unordered(self.gs_url,
                                                                         chunk_size=chunk_size,
                                                                         concurrency=concurrency):
                data[chunk_id * chunk_size: chunk_id * chunk_size + len(chunk)] = chunk
                chunk.release()
            self.assertEqual(self.expected_data, data)

if __name__ == '__main__':
    unittest.main()
