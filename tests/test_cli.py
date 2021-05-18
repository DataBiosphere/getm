#!/usr/bin/env python
import os
import sys
import unittest
import contextlib
from uuid import uuid4
from hashlib import md5
from unittest import mock
from tempfile import TemporaryDirectory

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from getm.cli import download, oneshot, multipart
from getm.checksum import MD5

from tests.infra import suppress_warnings
from tests.infra.server import ThreadedLocalServer, SilentHandler


class Server:
    server: ThreadedLocalServer
    host: str
    data = dict()

    @classmethod
    def set_data(cls, length: int):
        path = f"/{uuid4()}"
        cls.data[path] = os.urandom(length)
        return cls.host + path, cls.data[path]

class MockExecutor:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def submit(self, func, *args, **kwargs):
        func(*args, **kwargs)

def setUpModule():
    class Handler(SilentHandler):
        def do_HEAD(self, *args, **kwargs):
            data = Server.data[self.path]
            self.send_response(200)
            self.send_header("Content-Length", len(data))
            self.send_header("Content-MD5", md5(data).hexdigest())
            self.end_headers()

        def do_GET(self, *args, **kwargs):
            self.do_HEAD(*args, **kwargs)
            self.wfile.write(Server.data[self.path])

    Server.server = ThreadedLocalServer(Handler)
    Server.server.start()
    Server.host = Server.server.host

def tearDownModule():
    Server.server.shutdown()

@mock.patch("getm.cli.Progress")
class TestCLI(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def setUp(self):
        suppress_warnings()
        self.filepath = f"{self.temp_dir.name}/{uuid4()}"
        Server.data = dict()

    def test_download(self, *args):
        multipart_threshold = 7
        oneshot_sizes = 3 * [1]
        multipart_sizes = 5 * [1 + multipart_threshold]
        url_info = [dict(url=Server.set_data(size)[0], filepath=f"{self.temp_dir.name}/{uuid4()}")
                    for size in oneshot_sizes + multipart_sizes]

        with self.subTest("routing"):
            with mock.patch("getm.cli.ProcessPoolExecutor", MockExecutor):
                with mock.patch("getm.cli.oneshot") as mock_oneshot:
                    with mock.patch("getm.cli.multipart") as mock_multipart:
                        download(url_info, multipart_threshold=multipart_threshold)
                        self.assertEqual(len(oneshot_sizes), len(mock_oneshot.call_args_list))
                        self.assertEqual(len(multipart_sizes), len(mock_multipart.call_args_list))

        for oneshot_concurrency, multipart_concurrency in [(0, 0), (1, 0), (0, 1)]:
            with self.subTest("assertions",
                              oneshot_concurrency=oneshot_concurrency,
                              multipart_concurrency=multipart_concurrency):
                with self.assertRaises(AssertionError):
                    download(url_info, oneshot_concurrency, multipart_concurrency)

        with self.subTest("download"):
            download(url_info, multipart_threshold=multipart_threshold)
            for info in url_info:
                url = info['url']
                path = "/" + info['url'].rsplit("/", 1)[-1]
                with open(info['filepath'], "rb") as fh:
                    self.assertEqual(Server.data[path], fh.read())

    def test_oneshot(self, *args):
        url, expected_data = Server.set_data(1021)
        expected_cs, cs = md5(expected_data).hexdigest(), MD5()
        oneshot(url, self.filepath)
        with open(self.filepath, "rb") as fh:
            self.assertEqual(expected_data, fh.read())

    @mock.patch("getm.default_chunk_size", 1021)
    def test_multipart(self, *args):
        url, expected_data = Server.set_data(999983)
        expected_cs, cs = md5(expected_data).hexdigest(), MD5()
        multipart(url, self.filepath)
        with open(self.filepath, "rb") as fh:
            self.assertEqual(expected_data, fh.read())

if __name__ == '__main__':
    unittest.main()
