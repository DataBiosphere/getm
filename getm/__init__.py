from typing import Generator, Optional

from getm import reader


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
