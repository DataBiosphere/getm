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

from getm.utils import resolve_target


class TestUtils(unittest.TestCase):
    def test_resolve_target(self, *args):
        name = f"{uuid4()}"
        tests = [
            (os.path.join(os.path.abspath(os.path.curdir), name), None),
            (os.path.join(os.path.abspath(os.path.curdir), name), "."),
            (os.path.join(os.path.abspath(os.path.curdir), "foo"), "foo"),
            ("/doom/foo", "/doom/foo"),
            (os.path.expanduser("~/doom/foo"), "~/doom/foo"),
            (os.path.expanduser(f"~/{name}"), "~"),
        ]
        with mock.patch("getm.utils.http.name", return_value=name):
            for expected, filepath in tests:
                with self.subTest(expected=expected, filepath=filepath):
                    self.assertEqual(expected, resolve_target("http://foo", filepath))

if __name__ == '__main__':
    unittest.main()
