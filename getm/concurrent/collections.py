"""Provide objects to manage results from concurrent operations using Executor."""
from collections import deque
from multiprocessing import cpu_count
from concurrent.futures import Future, as_completed, wait, Executor, FIRST_COMPLETED
from typing import Any, Deque, List, Set, Callable, Generator, ContextManager


class _ConcurrentCollection:
    def __init__(self, executor: Executor, concurrency: int=0):
        self.executor = executor
        self.concurrency = concurrency
        assert 0 < self.concurrency

    def __len__(self):
        raise NotImplementedError()

    def __bool__(self) -> bool:
        return bool(len(self))

    def put(self, func: Callable, *args, **kwargs):
        raise NotImplementedError()

    def get(self) -> Any:
        raise NotImplementedError()

    def consume(self) -> Generator[Any, None, None]:
        raise NotImplementedError()

    def consume_finished(self) -> Generator[Any, None, None]:
        raise NotImplementedError()

    def running(self) -> List[Future]:
        return [f for f in getattr(self, "_futures", list()) if not f.done()]

    def abort(self):
        futures = getattr(self, "_futures", list())
        for f in futures:
            f.cancel()
        wait(futures)

    def __del__(self):
        self.abort()

class ConcurrentPool(_ConcurrentCollection):
    """Unordered collection providing results of concurrent operations. Up to 'concurrency' operations are executed in
    parallel.
    """
    def __init__(self, executor: Executor, concurrency: int=cpu_count()):
        super().__init__(executor, concurrency)
        self._futures: Set[Future] = set()

    def __len__(self):
        return len(self._futures)

    def put(self, func: Callable, *args, **kwargs):
        running = len(self.running())
        self._wait(running - self.concurrency)
        self._futures.add(self.executor.submit(func, *args, **kwargs))

    def get(self) -> Any:
        if self._futures:
            f = wait(self._futures, return_when=FIRST_COMPLETED).done.pop()
            self._futures.remove(f)
            return f.result()

    def __iter__(self) -> Any:
        while self._futures:
            for f in as_completed(self._futures):
                self._futures.remove(f)
                yield f.result()

    def _wait(self, count: int=-1):
        for f, _ in zip(as_completed(self._futures), range(count)):
            pass

class ConcurrentQueue(_ConcurrentCollection):
    """FIFO queue providing results of concurrent operations in the order they were submitted. Up to 'concurrency'
    operations are executed in parallel. New operations are executed concurrently as available results are consumed.
    """
    def __init__(self, executor: Executor, concurrency: int=cpu_count()):
        super().__init__(executor, concurrency)
        self._futures: Deque[Future] = deque()
        self._scheduled: Deque[Any] = deque()

    def __len__(self):
        return len(self._scheduled) + len(self._futures)

    def _submit(self):
        while len(self._futures) < self.concurrency and self._scheduled:
            func, args, kwargs = self._scheduled.popleft()
            self._futures.append(self.executor.submit(func, *args, **kwargs))

    def put(self, func: Callable, *args, **kwargs):
        self._scheduled.append((func, args, kwargs))
        self._submit()

    def get(self) -> Any:
        res = self._futures.popleft().result() if self._futures else None
        self._submit()
        return res

    def __iter__(self) -> Generator[Any, None, None]:
        while self._scheduled or self._futures:
            yield self.get()
