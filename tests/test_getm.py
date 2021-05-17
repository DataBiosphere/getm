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

import getm
from tests.infra import GS, S3, suppress_warnings


class TestGetm(unittest.TestCase):
    def test_dispatch(self):
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch("getm.reader.SharedCircularBuffer"))
            stack.enter_context(mock.patch("getm.reader.ProcessPoolExecutor"))
            stack.enter_context(mock.patch("getm.reader.ConcurrentQueue"))
            stack.enter_context(mock.patch("getm.reader.ConcurrentPool"))
            stack.enter_context(mock.patch("getm.reader.http"))

            tests = [(None, getm.reader.URLRawReader),
                     (1, getm.reader.URLReaderKeepAlive),
                     (4, getm.reader.URLReader)]
            for concurrency, expected_class in tests:
                with self.subTest(concurrency=concurrency, expected_class=expected_class):
                    obj = getm.urlopen("http://this-is-fake-i-hope-xyz", concurrency=concurrency)
                    self.assertIsInstance(obj, expected_class)

            tests = [(None, getm.reader.URLRawReader.iter_content),
                     (1, getm.reader.URLReaderKeepAlive.iter_content),
                     (3, getm.reader.URLReader.iter_content)]

            for concurrency, expected_func in tests:
                with self.subTest(concurrency=concurrency, expected_func=expected_func):
                    obj = getm.iter_content("http://this-is-fake-i-hope-xyz", concurrency=concurrency)
                    self.assertEqual(obj.__name__, expected_func.__name__)

if __name__ == '__main__':
    unittest.main()
