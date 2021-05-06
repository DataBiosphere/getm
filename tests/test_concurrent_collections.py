#!/usr/bin/env python
import io
import os
import sys
import time
import random
import unittest
from concurrent.futures import ThreadPoolExecutor

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from getm.concurrent import ConcurrentPool, ConcurrentQueue


class ConcurrentCollectionTests:
    ac_class = None

    def setUp(self):
        self.executor = ThreadPoolExecutor()

    def tearDown(self):
        self.executor.shutdown()

    def test_abort(self):
        fs = self.ac_class(self.executor)
        start_time = time.time()
        numbers = [2,3,5,7,11,13,17]
        for cs in numbers:
            fs.put(_wait_and_return, cs)
        self.assertEqual(len(fs), len(numbers))
        fs.abort()
        self.assertLessEqual(start_time - time.time(), numbers[-1])

    def test_exceptions(self):
        def func_that_raises(*args, **kwargs):
            raise RuntimeError("doom")

        with self.subTest("get should raise"):
            fs = self.ac_class(self.executor)
            fs.put(func_that_raises)
            with self.assertRaises(RuntimeError):
                fs.get()

        with self.subTest("iteration should raise"):
            fs = self.ac_class(self.executor)
            fs.put(func_that_raises)
            with self.assertRaises(RuntimeError):
                [a for a in fs]

class TestConcurrentPool(ConcurrentCollectionTests, unittest.TestCase):
    ac_class = ConcurrentPool

    def test_normal(self):
        numbers = [2,3,5,7,11,13,17]
        with self.subTest("put and get"):
            fs = ConcurrentPool(self.executor)
            for n in numbers:
                fs.put(_wait_and_return, n)
            returned_numbers = list()
            while fs:
                returned_numbers.append(fs.get())
            self.assertEqual(sorted(returned_numbers), sorted(numbers))

        with self.subTest("put and iterate"):
            fs = ConcurrentPool(self.executor)
            for n in numbers:
                fs.put(_wait_and_return, n)
            returned_numbers = [n for n in fs]
            self.assertEqual(sorted(returned_numbers), sorted(numbers))

    def test_block_on_put(self):
        numbers = [2,3,5]
        with self.subTest("block if concurrency >= 1"):
            fs = ConcurrentPool(self.executor, concurrency=2)
            start_time = time.time()
            for n in numbers:
                fs.put(_wait_and_return, n, wait=2)
            self.assertGreater(time.time() - start_time, 2)
            self.assertEqual(sorted(numbers), sorted([n for n in fs]))
        with self.subTest("raise if concurrency <= 0"):
            with self.assertRaises(AssertionError):
                fs = ConcurrentPool(self.executor, concurrency=0)
            with self.assertRaises(AssertionError):
                fs = ConcurrentPool(self.executor, concurrency=-1)

class TestConcurrentQueue(ConcurrentCollectionTests, unittest.TestCase):
    ac_class = ConcurrentQueue

    def test_normal(self):
        numbers = [2,3,5,7,11,13,17]
        with self.subTest("put and get"):
            fs = ConcurrentQueue(self.executor)
            for n in numbers:
                fs.put(_wait_and_return, n)
            returned_numbers = list()
            while fs:
                returned_numbers.append(fs.get())
            self.assertEqual(returned_numbers, numbers)
        with self.subTest("put and iterate"):
            fs = ConcurrentQueue(self.executor)
            for n in numbers:
                fs.put(_wait_and_return, n)
            returned_numbers = [n for n in fs]
            self.assertEqual(returned_numbers, numbers)

    def test_limited_execution(self):
        numbers = [2,3,5]
        with self.subTest("limit execution to concurrency"):
            fs = ConcurrentQueue(self.executor, concurrency=2)
            for n in numbers:
                fs.put(_wait_and_return, n)
            self.assertEqual(2, len(fs._futures))
        with self.subTest("raise if concurrency <= 0"):
            with self.assertRaises(AssertionError):
                fs = ConcurrentQueue(self.executor, concurrency=0)
            with self.assertRaises(AssertionError):
                fs = ConcurrentQueue(self.executor, concurrency=-1)

def _wait_and_return(i, wait=None):
    wait = wait or random.random() / 2
    time.sleep(wait)
    return i

if __name__ == '__main__':
    unittest.main()
