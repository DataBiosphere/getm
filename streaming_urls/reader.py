import io
from math import ceil
from functools import lru_cache
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, Tuple, Generator

from streaming_urls.http import http_session
from streaming_urls.config import default_chunk_size
from streaming_urls.concurrent import ConcurrentQueue, ConcurrentPool, SharedCircularBuffer, SharedBufferArray


http = http_session()

class Reader(io.IOBase):
    """
    Chunks are downloaded with concurrency equal to `concurrency`.

    Concurrency can be disabled by passing in`concurrency=None`.
    """
    def __init__(self, url: str, chunk_size: int=default_chunk_size, concurrency: Optional[int]=3):
        assert chunk_size >= 1
        self.url = url
        self.size = http.size(url)
        self.chunk_size = chunk_size
        if concurrency is not None:
            assert 1 <= concurrency
            self._start = self._stop = 0
            self._buf = SharedCircularBuffer(size=(2 * concurrency + 1) * chunk_size, create=True)
            self.max_read = concurrency * chunk_size
            self.executor = ProcessPoolExecutor(max_workers=concurrency)
            self.future_parts = ConcurrentQueue(self.executor, concurrency=concurrency)
            for part_coord in part_coords(self.size, self.chunk_size):
                self.future_parts.put(_fetch_part, self.url, *part_coord, self._buf.name)
        else:
            self._raw_stream = http.raw(url)
            self.max_read = self.size

    def readable(self):
        return True

    def read(self, sz: int=-1) -> memoryview:
        """
        Read at most 'sz' bytes from stream and provide a 'memoryview'.. The user is expected to call 'release' on the
        view to facilitate program exit.
        """
        if -1 == sz:
            sz = self.size
        if hasattr(self, "_raw_stream"):
            return memoryview(self._raw_stream.read(sz))
        else:
            # avoid overwite in circular buffer
            sz = min(sz, self.max_read)
            while sz > self._stop - self._start and len(self.future_parts):
                _, _, part_size = self.future_parts.get()
                self._stop += part_size
            sz = min(sz, self._stop - self._start)  # don't overflow end of data
            if sz:
                res = self._buf[self._start: self._start + sz]
                self._start += len(res)
            else:
                res = memoryview(bytes())
            return res

    def readinto(self, buff: bytearray) -> int:
        if hasattr(self, "_raw_stream"):
            return self._raw_stream.readinto(buff)
        else:
            d = self.read(len(buff))
            bytes_read = len(d)
            buff[:bytes_read] = d
            d.release()
            return bytes_read

    def seek(self, *args, **kwargs):
        raise OSError()

    def tell(self, *args, **kwargs):
        raise NotImplementedError()

    def truncate(self, *args, **kwargs):
        raise NotImplementedError()

    def write(self, *args, **kwargs):
        raise OSError()

    def close(self):
        if hasattr(self, "_raw_stream"):
            self._raw_stream.close()
        if hasattr(self, "executor"):
            self.executor.shutdown()
        if hasattr(self, "_buf"):
            self._buf.close()
        super().close()

def _number_of_parts(size: int, chunk_size: int) -> int:
    return ceil(size / chunk_size)

def _part_range(size: int, chunk_size: int, part_id: int) -> Tuple[int, int]:
    start = part_id * chunk_size
    if part_id == (_number_of_parts(size, chunk_size) - 1):
        part_size = size % chunk_size or chunk_size
    else:
        part_size = chunk_size
    return start, part_size

def part_coords(size: int, chunk_size: int):
    for part_id in range(_number_of_parts(size, chunk_size)):
        start, part_size = _part_range(size, chunk_size, part_id)
        yield part_id, start, part_size

def _fetch_part(url: str, part_id: int, start: int, part_size: int, sb_name: str) -> Tuple[int, int, int]:
    # This method is executed in subprocesses. Rerferences to global variables should be avoided.
    # See https://docs.python.org/3/library/multiprocessing.html#programming-guidelines
    buf = SharedCircularBuffer(sb_name)
    http_session().get_range_readinto(url, start, part_size, buf[start: start + part_size])
    return part_id, start, part_size

def for_each_part(url: str, chunk_size: int=default_chunk_size, concurrency: Optional[int]=3):
    """
    Fetch chunks and yield in order. Chunks are downloaded with concurrency equal to `concurrency`
    Chunks are 'memoryview' objects that may be pointing to multiprocessing shared memory.
    It is the responsibility of the user to call 'release' on each chunk to facilitate program exit.
    """
    if concurrency is not None:
        assert 1 <= concurrency
        size = http.size(url)
        with SharedCircularBuffer(size=chunk_size * concurrency, create=True) as buff:
            with ProcessPoolExecutor(max_workers=concurrency) as e:
                future_parts = ConcurrentQueue(e, concurrency=concurrency)
                for part_coord in part_coords(size, chunk_size):
                    future_parts.put(_fetch_part, url, *part_coord, buff.name)
                for part_id, start, part_size in future_parts:
                    yield buff[start: start + part_size]
    else:
        for chunk in http.iter_content(url, chunk_size=chunk_size):
            yield memoryview(chunk)

def _fetch_part_ar(url: str,
                   part_id: int,
                   start: int,
                   part_size: int,
                   sb_name: str,
                   sb_index: int) -> Tuple[int, int, int, int]:
    # This method is executed in subprocesses. Rerferences to global variables should be avoided.
    # See https://docs.python.org/3/library/multiprocessing.html#programming-guidelines
    buf = SharedBufferArray(sb_name)
    http_session().get_range_readinto(url, start, part_size, buf[sb_index][:part_size])
    return part_id, start, part_size, sb_index

def for_each_part_async(url: str,
                        chunk_size: int=default_chunk_size,
                        concurrency: int=3) -> Generator[Tuple[int, bytes], None, None]:
    """
    Fetch chunks with concurrency equal to `concurrency`, yielding results as soon as available.
    Results may be returned in any order.
    Chunks are 'memoryview' objects that may be pointing to multiprocessing shared memory.
    It is the responsibility of the user to call 'release' on each chunk to facilitate program exit.
    """
    assert 1 <= concurrency
    size = http.size(url)
    parts_to_fetch = deque(part_coords(size, chunk_size))
    with SharedBufferArray(chunk_size=chunk_size, num_chunks=concurrency, create=True) as buff:
        with ProcessPoolExecutor(max_workers=concurrency) as e:
            future_parts = ConcurrentPool(e, concurrency)
            for i in range(concurrency):
                future_parts.put(_fetch_part_ar, url, *parts_to_fetch.popleft(), buff.name, i)
            for part_id, start, part_size, i in future_parts:
                yield part_id, buff[i][:part_size]
                if parts_to_fetch:
                    future_parts.put(_fetch_part_ar, url, *parts_to_fetch.popleft(), buff.name, i)
