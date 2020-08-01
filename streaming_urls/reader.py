import io
import requests
from math import ceil
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Tuple, Generator

from streaming_urls.config import default_chunk_size, reader_retries
from streaming_urls.async_collections import AsyncQueue, AsyncPool


class Reader(io.IOBase):
    """
    Readable stream on top of GS blob. Bytes are fetched in chunks of `chunk_size`.
    Chunks are downloaded with concurrency equal to `threads`.

    Concurrency can be disabled by passing in`threads=None`.
    """
    def __init__(self, url: str, chunk_size: int=default_chunk_size, threads: Optional[int]=1):
        assert chunk_size >= 1
        self.url = url
        self.chunk_size = chunk_size
        self._buffer = bytearray()
        self._pos = 0
        self.size = _get_size(url)
        self.number_of_chunks = ceil(self.size / self.chunk_size)
        self._unfetched_chunks = [i for i in range(self.number_of_chunks)]
        if threads is not None:
            assert 1 <= threads
            self.executor = ThreadPoolExecutor(max_workers=threads)
            self.future_chunk_downloads = AsyncQueue(self.executor, concurrency=threads)
            for chunk_number in self._unfetched_chunks:
                self.future_chunk_downloads.put(self._fetch_chunk, chunk_number)
        else:
            resp = requests.get(url, stream=True)
            resp.raise_for_status()
            self._raw_stream = resp.raw

    def _fetch_chunk(self, chunk_number: int) -> bytes:
        start_chunk = chunk_number * self.chunk_size
        end_chunk = start_chunk + self.chunk_size - 1
        if chunk_number == (self.number_of_chunks - 1):
            expected_part_size = self.size % self.chunk_size or self.chunk_size
        else:
            expected_part_size = self.chunk_size
        return _download_chunk_from_url(self.url, start_chunk, end_chunk, expected_part_size)

    def readable(self):
        return True

    def read(self, size: int=-1) -> bytes:
        if -1 == size:
            size = self.size
        if hasattr(self, "_raw_stream"):
            return self._raw_stream.read(size)
        else:
            if size + self._pos > len(self._buffer):
                del self._buffer[:self._pos]
                while size > len(self._buffer) and len(self.future_chunk_downloads):
                    self._buffer += self.future_chunk_downloads.get()
                self._pos = 0
            ret_data = bytes(memoryview(self._buffer)[self._pos:self._pos + size])
            self._pos += len(ret_data)
            return ret_data

    def readinto(self, buff: bytearray) -> int:
        if hasattr(self, "_raw_stream"):
            return self._raw_stream.readinto(buff)
        else:
            d = self.read(len(buff))
            bytes_read = len(d)
            buff[:bytes_read] = d
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
        super().close()

def _get_size(url: str) -> int:
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    return int(resp.headers['Content-Length'])

def for_each_chunk(url: str, chunk_size: int=default_chunk_size, threads: Optional[int]=1):
    """
    Fetch chunks and yield in order. Chunks are downloaded with concurrency equal to `threads`
    """
    reader = Reader(url, chunk_size=chunk_size)
    if threads is not None:
        assert 1 <= threads
        with ThreadPoolExecutor(max_workers=threads) as e:
            future_chunk_downloads = AsyncQueue(e, concurrency=threads)
            for chunk_number in reader._unfetched_chunks:
                future_chunk_downloads.put(reader._fetch_chunk, chunk_number)
            for chunk in future_chunk_downloads.consume():
                yield chunk
    else:
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=chunk_size):
            yield chunk

def for_each_chunk_async(url: str,
                         chunk_size: int=default_chunk_size,
                         threads: int=2) -> Generator[Tuple[int, bytes], None, None]:
    """
    Fetch chunks with concurrency equal to `threads`, yielding results as soon as available.
    Results may be returned in any order.
    """
    assert 1 <= threads
    reader = Reader(url, chunk_size)

    def fetch_chunk(chunk_number):
        chunk = reader._fetch_chunk(chunk_number)
        return chunk_number, chunk

    with ThreadPoolExecutor(max_workers=threads) as e:
        chunks = AsyncPool(e, threads)
        for fetch_chunk_number in range(reader.number_of_chunks):
            for chunk_number, chunk in chunks.consume_finished():
                yield chunk_number, chunk
                break
            chunks.put(fetch_chunk, fetch_chunk_number)
        for chunk_number, chunk in chunks.consume():
            yield chunk_number, chunk

def _download_chunk_from_url(url: str, start: int, end: int, expected_size: Optional[int]=None) -> bytes:
    if expected_size is None:
        resp = requests.get(url, headers=dict(Range=f"bytes={start}-{end}"))
        resp.raise_for_status()
        return resp.content
    else:
        for _ in range(reader_retries):
            resp = requests.get(url, headers=dict(Range=f"bytes={start}-{end}"))
            resp.raise_for_status()
            if expected_size == len(resp.content):
                return resp.content
        raise ValueError("Unexpected part size")
