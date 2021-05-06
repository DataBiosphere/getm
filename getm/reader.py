import io
from math import ceil
from collections import deque
from multiprocessing import Process, Pipe
from concurrent.futures import ProcessPoolExecutor
from typing import Optional, Generator, Tuple

from getm.http import http_session
from getm.concurrent import ConcurrentQueue, ConcurrentPool, SharedCircularBuffer, SharedBufferArray


http = http_session()

class BaseURLReader(io.IOBase):
    def readable(self):
        return True

    def read(self, sz: int=-1) -> memoryview:
        raise NotImplementedError()

    def readinto(self, buff: bytearray) -> int:
        raise NotImplementedError()

    def seek(self, *args, **kwargs):
        raise OSError()

    def tell(self, *args, **kwargs):
        raise NotImplementedError()

    def truncate(self, *args, **kwargs):
        raise NotImplementedError()

    def write(self, *args, **kwargs):
        raise OSError()

class URLRawReader(BaseURLReader):
    def __init__(self, url: str):
        self.size = http.size(url)
        self.handle = http.raw(url)

    def read(self, sz: int=-1) -> memoryview:
        if -1 == sz:
            sz = self.size
        return memoryview(self.handle.read(sz))

    def readinto(self, buff: bytearray) -> int:
        return self.handle.readinto(buff)

    def close(self):
        self.handle.close()
        super().close()

    @classmethod
    def iter_content(cls, url: str, chunk_size: int):
        for part_id, part in enumerate(http.iter_content(url, chunk_size=chunk_size)):
            yield memoryview(part)

class URLReader(BaseURLReader):
    """
    Provide a streaming object to bytes referenced by 'url'. Chunks of data are pre-fetched in the background with
    concurrency='concurrency'.
    """
    def __init__(self, url: str, chunk_size: int, concurrency: int):
        assert chunk_size >= 1
        assert 1 <= concurrency
        self.url = url
        self.size = http.size(url)
        self.chunk_size = chunk_size
        self._start = self._stop = 0
        self._buf = SharedCircularBuffer(size=(2 * concurrency + 1) * chunk_size, create=True)
        self.max_read = concurrency * chunk_size
        self.executor = ProcessPoolExecutor(max_workers=concurrency)
        self.future_parts = ConcurrentQueue(self.executor, concurrency=concurrency)
        for part_coord in part_coords(self.size, self.chunk_size):
            self.future_parts.put(self._fetch_part, self.url, *part_coord, self._buf.name)

    def read(self, sz: int=-1) -> memoryview:
        """
        Read at most 'sz' bytes from stream as a memoryview object referencing multiprocessing shared memory. The
        caller is expected to call 'release' on each such object.
        """
        if -1 == sz:
            sz = self.size
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
        d = self.read(len(buff))
        bytes_read = len(d)
        buff[:bytes_read] = d
        d.release()
        return bytes_read

    def close(self):
        self.executor.shutdown()
        self._buf.close()
        super().close()

    @classmethod
    def iter_content(cls, url: str, chunk_size: int, concurrency: int) -> Generator[memoryview, None, None]:
        """
        Fetch parts and yield in order, pre-fetching with concurrency equal to `concurrency`. Parts are 'memoryview'
        objects that reference multiprocessing shared memory. The caller is expected to call 'release' on each part.
        """
        with cls(url, chunk_size, concurrency) as reader:
            for part_id, start, part_size in reader.future_parts:
                yield reader._buf[start: start + part_size]

    @staticmethod
    def _fetch_part(url: str, part_id: int, start: int, part_size: int, sb_name: str) -> Tuple[int, int, int]:
        # This method is executed in subprocesses. Rerferences to global variables should be avoided.
        # See https://docs.python.org/3/library/multiprocessing.html#programming-guidelines
        buf = SharedCircularBuffer(sb_name)
        http_session().get_range_readinto(url, start, part_size, buf[start: start + part_size])
        return part_id, start, part_size

class URLReaderKeepAlive(BaseURLReader, Process):
    def __init__(self, url: str, chunk_size: int):
        self.url = url
        self.chunk_size = chunk_size
        self.pipe, self.pipesp = Pipe()
        self.size = http.size(url)
        self._start = self._stop = 0
        self.max_read = 40 * self.chunk_size
        self._buf = SharedCircularBuffer(size=chunk_size + self.max_read, create=True)
        super().__init__()

    def run(self):
        handle = http_session().raw(self.url)
        start = stop = 0
        with SharedCircularBuffer(self._buf.name) as buf:
            while True:
                while stop - start + self.chunk_size >= buf.size:
                    # If there's no more room in the buffer, wait for the reader
                    try:
                        start = self.pipesp.recv()
                        if "STOP" == start:
                            return
                    except EOFError:
                        break
                bytes_read = handle.readinto(buf[stop: stop + self.chunk_size])
                if not bytes_read:
                    break
                stop += bytes_read
                self.pipesp.send(stop)

    def read(self, sz: int=-1):
        if -1 == sz:
            sz = self.max_read
        self.pipe.send(self._start)
        sz = min(sz, self.max_read)
        while sz > self._stop - self._start and self._stop < self.size:
            self._stop = self.pipe.recv()
        sz = min(sz, self._stop - self._start)
        if sz:
            res = self._buf[self._start: self._start + sz]
            self._start += len(res)
            return res
        else:
            return memoryview(bytes())

    def readinto(self, buff: bytearray) -> int:
        d = self.read(len(buff))
        bytes_read = len(d)
        buff[:bytes_read] = d
        d.release()
        return bytes_read

    def close(self):
        self.pipe.send("STOP")
        self.pipe.close()
        self.pipesp.close()
        self.join(timeout=5)
        self._buf.close()
        super().close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.close()

    @classmethod
    def iter_content(cls, url: str, chunk_size: int) -> Generator[memoryview, None, None]:
        """
        Fetch parts and yield in order, pre-fetching with concurrency equal to `concurrency`. Parts are 'memoryview'
        objects that reference multiprocessing shared memory. The caller is expected to call 'release' on each part.
        """
        with cls(url, chunk_size) as reader:
            start = stop = 0
            while True:
                while stop - start < reader.chunk_size and stop < reader.size:
                    stop = reader.pipe.recv()
                read_length = min(reader.chunk_size, stop - start)
                if not read_length:
                    break
                res = reader._buf[start: start + read_length]
                start += read_length
                yield res
                reader.pipe.send(start)

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

def _fetch_part_uo(url: str,
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

def iter_content_unordered(url: str,
                           chunk_size: int,
                           concurrency: int) -> Generator[Tuple[int, memoryview], None, None]:
    """
    Fetch parts and yield in any order, pre-fetching with concurrency equal to `concurrency`. Parts are 'memoryview'
    objects that reference multiprocessing shared memory. The caller is expected to call 'release' on each part.
    This method may offer slightly better performance than 'iter_content'.
    """
    assert 1 <= concurrency
    size = http.size(url)
    parts_to_fetch = deque(part_coords(size, chunk_size))
    with SharedBufferArray(chunk_size=chunk_size, num_chunks=concurrency, create=True) as buff:
        with ProcessPoolExecutor(max_workers=concurrency) as e:
            future_parts = ConcurrentPool(e, concurrency)
            for i in range(concurrency):
                future_parts.put(_fetch_part_uo, url, *parts_to_fetch.popleft(), buff.name, i)
            for part_id, start, part_size, i in future_parts:
                yield part_id, buff[i][:part_size]
                if parts_to_fetch:
                    future_parts.put(_fetch_part_uo, url, *parts_to_fetch.popleft(), buff.name, i)
