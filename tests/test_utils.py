#!/usr/bin/env python
import os
import sys
import unittest
from uuid import uuid4
from unittest import mock
from tempfile import TemporaryDirectory

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from getm.utils import resolve_target, available_shared_memory


class TestUtils(unittest.TestCase):
    def test_resolve_target(self, *args):
        name = f"{uuid4()}"
        cwd = os.getcwd()
        with TemporaryDirectory() as tmpdir:
            tests = [
                (os.path.join(cwd, name), None),
                (os.path.join(cwd, name), "."),
                (os.path.join(cwd, "foo"), "foo"),
                (f"{tmpdir}/doom/foo", f"{tmpdir}/doom/foo"),
                (f"{tmpdir}/doom/foo/{name}", f"{tmpdir}/doom/foo/"),
                (os.path.expanduser("~/doom/foo"), "~/doom/foo"),
                (os.path.expanduser(f"~/{name}"), "~"),
            ]
            with mock.patch("getm.utils.http.name", return_value=name):
                for expected, filepath in tests:
                    with self.subTest(expected=expected, filepath=filepath):
                        target = resolve_target("http://foo", filepath)
                        self.assertEqual(expected, target)
                        self.assertTrue(os.path.isdir(os.path.dirname(target)))

    def test_available_shared_memory(self):
        shm_sz = available_shared_memory()
        if "darwin" == sys.platform:
            self.assertEqual(-1, shm_sz)

if __name__ == '__main__':
    unittest.main()
