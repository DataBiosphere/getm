#!/usr/bin/env python
import os
import sys
import unittest
from random import randint
from concurrent.futures import ProcessPoolExecutor, as_completed

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from getm.concurrent import SharedBufferArray


def _write(sb_name, chunk_id, chunk_content):
    with SharedBufferArray(sb_name) as sb:
        sb[chunk_id][:] = chunk_content

class TestSharedBufferArray(unittest.TestCase):
    def test_foo(self):
        num_chunks, chunk_size = 4, 5
        expected = b"".join([os.urandom(chunk_size) for _ in range(num_chunks)])

        with SharedBufferArray(chunk_size=chunk_size, num_chunks=num_chunks, create=True) as sb:
            with ProcessPoolExecutor(max_workers=num_chunks) as e:
                futures = [None] * num_chunks
                for i in range(num_chunks):
                    chunk = expected[i * chunk_size: (i + 1) * chunk_size]
                    futures[i] = e.submit(_write, sb.name, i, chunk)
                for f in as_completed(futures):
                    f.result()
            self.assertEqual(expected, bytes(sb._shared_memory.buf[:num_chunks * chunk_size]))

if __name__ == '__main__':
    unittest.main()
