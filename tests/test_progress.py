#!/usr/bin/env python
import os
import sys
import time
import unittest
from random import randint

import boto3

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from getm.progress import Chunker


class TestProgress(unittest.TestCase):
    def test_chunker(self):
        class CallbackInfo:
            size = 0
            progress = 0
            num_calls = 0

        def on_chunk_complete(size, progress, duration):
            CallbackInfo.size = size
            CallbackInfo.progress = progress
            CallbackInfo.num_calls += 1

        size = 31
        vals = [(7, 0, 0, 0),
                (7, 14, 1, size),
                (7, 21, 2, size),
                (7, 21, 2, size),
                (3, 31, 3, size)]
        c = Chunker(size, size / 3, on_chunk_complete)
        for chunk, expected_progress, expected_calls, expected_size in vals:
            c.add(chunk)
            self.assertEqual(expected_calls, CallbackInfo.num_calls)
            self.assertEqual(expected_progress, CallbackInfo.progress)
            self.assertEqual(expected_size, CallbackInfo.size)
        with self.assertRaises(ValueError):
            c.add(1)

if __name__ == '__main__':
    unittest.main()
