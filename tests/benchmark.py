#!/usr/bin/env python
import os
import sys
import base64
import hashlib
import unittest
from contextlib import contextmanager

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from getm import urlopen, iter_content, default_chunk_size, reader, checksum
from tests.infra import GS, S3, suppress_warnings
from tests.infra.profile import Profile


def setUpModule():
    suppress_warnings()
    GS.setup()
    S3.setup()

def tearDownModule():
    GS.client._http.close()

class TestBenchmark(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key, cls.size = GS.put_fixture()
        cls.expected_md5 = GS.bucket.get_blob(cls.key).md5_hash
        cls.expected_crc32c = GS.bucket.get_blob(cls.key).crc32c

    def setUp(self):
        self.url = GS.generate_presigned_GET_url(self.key)

    def tearDown(self):
        print()  # inserting a space between profile results improves readability

    def test_read(self):
        for concurrency in [None, 4]:
            with Profile(f"{self.id()} concurrency={concurrency}"):
                cs = checksum.GSCRC32C()
                with urlopen(self.url, concurrency=concurrency) as raw:
                    cs.update(raw.read())
                    while True:
                        d = raw.read()
                        cs.update(d)
                        if not d:
                            break
                self.assertTrue(cs.matches(self.expected_crc32c))

    def test_read_keep_alive(self):
        chunk_size = 1024 * 1024 * 1
        with Profile(f"{self.id()} chunk_size={chunk_size}"):
            cs = checksum.GSCRC32C()
            with urlopen(self.url, concurrency=1) as raw:
                while True:
                    d = raw.read(chunk_size)
                    if not d:
                        break
                    cs.update(d)
            self.assertTrue(cs.matches(self.expected_crc32c))

    def test_iter_content(self):
        for concurrency in [None, 4]:
            with Profile(f"{self.id()} concurrency={concurrency}"):
                cs = checksum.GSCRC32C()
                for chunk in iter_content(self.url, concurrency=concurrency):
                    cs.update(chunk)
                    chunk.release()
                self.assertTrue(cs.matches(self.expected_crc32c))

    def test_iter_content_keep_alive(self):
        concurrency = 1
        with Profile(f"{self.id()} concurrency={concurrency}"):
            cs = checksum.GSCRC32C()
            for chunk in iter_content(self.url, concurrency=concurrency):
                cs.update(chunk)
                chunk.release()
            self.assertTrue(cs.matches(self.expected_crc32c))

    def test_iter_content_unordered(self):
        tests = [(f"concurrency={concurrency}", concurrency) for concurrency in range(2,5)]
        for concurrency in range(3, 5):
            with Profile(f"{self.id()} concurrency={concurrency}"):
                cs = checksum.GSCRC32C()
                for i, chunk in reader.iter_content_unordered(self.url, default_chunk_size, concurrency):
                    cs.update(chunk)
                    chunk.release()

if __name__ == '__main__':
    unittest.main()
