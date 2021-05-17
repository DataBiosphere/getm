import os
from typing import Generator, Optional

from getm import reader
from getm.http import http
from getm.utils import checksum_for_url


default_chunk_size = 128 * 1024 * 1024
default_chunk_size_keep_alive = 1 * 1024 * 1024
default_concurrency = 4

def urlopen(url: str, concurrency: Optional[int]=None) -> reader.BaseURLReader:
    if concurrency is None:
        return reader.URLRawReader(url)
    elif 1 == concurrency:
        return reader.URLReaderKeepAlive(url, default_chunk_size_keep_alive)
    else:
        return reader.URLReader(url, default_chunk_size, concurrency or default_concurrency)

def iter_content(url: str, concurrency: Optional[int]=None) -> Generator[memoryview, None, None]:
    if concurrency is None:
        return reader.URLRawReader.iter_content(url, default_chunk_size)
    elif 1 == concurrency:
        return reader.URLReaderKeepAlive.iter_content(url, default_chunk_size_keep_alive)
    else:
        return reader.URLReader.iter_content(url, default_chunk_size, concurrency or default_concurrency)

def download_iter_parts(url: str,
                        filepath: str,
                        chunk_size: Optional[int]=None,
                        concurrency: Optional[int]=1) -> Generator[int, None, None]:
    expected_cs, cs = checksum_for_url(url)
    assert cs is not None
    sz = http.size(url)
    if not chunk_size:
        chunk_size = default_chunk_size if 1 != concurrency else default_chunk_size_keep_alive
    try:
        fd = os.open(filepath, os.O_WRONLY | os.O_CREAT)
        os.pwrite(fd, b"0", sz - 1)
        offset = 0
        for part in iter_content(url, concurrency):
            try:
                cs.update(bytes(part))
                os.pwrite(fd, part, offset)
                part_size = len(part)
                offset += part_size
            finally:
                part.release()
            yield part_size
    except Exception:
        os.remove(filepath)
        raise
    finally:
        os.close(fd)
    if expected_cs is not None:
        assert cs.matches(expected_cs), "checksum failed!"
