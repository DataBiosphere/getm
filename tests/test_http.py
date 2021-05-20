#!/usr/bin/env python
import io
import os
import sys
import requests
import unittest
import warnings
from uuid import uuid4
from random import randint

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from getm.http import HTTPAdapter, Retry, http_session

from tests.infra import GS, S3, suppress_warnings
from tests.infra.server import ThreadedLocalServer, SilentHandler


def setUpModule():
    suppress_warnings()
    GS.setup()
    S3.setup()

def tearDownModule():
    GS.client._http.close()

class HeadHandler(SilentHandler):
    test_headers = dict()

    def do_HEAD(self, *args, **kwargs):
        self.send_response(200)
        for k, v in self.test_headers.items():
            if v is not None:
                self.send_header(k, v)
        self.end_headers()

    def do_GET(self, *args, **kwargs):
        self.do_HEAD(*args, **kwargs)

class TestHTTP(unittest.TestCase):
    def setUp(self):
        suppress_warnings()

    @classmethod
    def setUpClass(cls):
        cls.key = f"test_read/{uuid4()}"
        cls.expected_data = os.urandom(randint(8, 16))
        S3.bucket.Object(cls.key).upload_fileobj(io.BytesIO(cls.expected_data))
        GS.bucket.blob(cls.key).upload_from_file(io.BytesIO(cls.expected_data))
        cls.s3_url = S3.generate_presigned_GET_url(cls.key)
        cls.gs_url = GS.generate_presigned_GET_url(cls.key)

    def setUp(self):
        # Suppress warnings of the form 'ResourceWarning: unclosed <socket.socket' so they don't muck up test output
        # It'd sure be nice to nice to know how to avoid these things in the first place.
        warnings.filterwarnings("ignore", category=ResourceWarning)

    def test_retry(self):
        class Handler(SilentHandler):
            def do_GET(self, *args, **kwargs):
                self.send_response(500)
                self.end_headers()

        with ThreadedLocalServer(Handler) as host:
            expected_recount = 3
            retry_count = dict(count=0)

            class TestRetry(Retry):
                def increment(self, *args, **kwargs):
                    retry_count['count'] += 1
                    return super().increment(*args, **kwargs)

            with http_session(retry=TestRetry(total=expected_recount - 1,
                                              status_forcelist=[500],
                                              allowed_methods=["GET"])) as http:
                try:
                    http.get(host)
                except requests.exceptions.RetryError:
                    pass

            self.assertEqual(expected_recount, retry_count['count'])

    def test_size(self):
        expected_size = len(self.expected_data)
        for platform, url in [("aws", self.s3_url), ("gcp", self.gs_url)]:
            with self.subTest(platform=platform):
                with http_session() as http:
                    self.assertEqual(expected_size, http.size(url))

    def test_name(self):
        with ThreadedLocalServer(HeadHandler) as host:
            tests = [
                ("xyzy-1", "bar; filename=\"xyzy-1\"; blah", f"{host}/foof"),
                ("xyzy-2", "foo; bar filename=\"asf\"; blah", f"{host}/xyzy-2"),
                ("xyzy-3", "foo; filename=; blah", f"{host}/foo/xyzy-3"),
                ("xyzy-4", None, f"{host}/foo/xyzy-4"),
            ]
            with http_session() as http:
                for expected, content_disp, url in tests:
                    HeadHandler.test_headers['Content-Disposition'] = content_disp
                    with self.subTest(expected_name=expected, content_disp=content_disp, url=url):
                        self.assertEqual(expected, http.name(url))

                with self.subTest("Should raise for indeterminate name"):
                    with self.assertRaises(ValueError):
                        url, HeadHandler.content_disp = host, None
                        http.name(url)

    def test_checksums(self):
        tests = [
            (dict(gs_crc32c="a", gs_md5="b", etag="c"), {'x-goog-hash': "crc32c=a, md5=b", 'ETag': "c"}),
            (dict(s3_etag="d"), {'Server': "AmazonS3", 'ETag': "d"}),
            (dict(md5="e"), {'Content-MD5': "e"}),
            (dict(), {}),
        ]

        with ThreadedLocalServer(HeadHandler) as host:
            with http_session() as http:
                for expected, test_headers in tests:
                    HeadHandler.test_headers = test_headers
                    self.assertEqual(expected, http.checksums(f"{host}/{uuid4()}"))

if __name__ == '__main__':
    unittest.main()
