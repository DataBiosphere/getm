from typing import Generator, Optional, Tuple

from streaming_urls.reader import BaseURLReader, URLRawReader, URLReader, URLReaderKeepAlive, iter_content_unordered


default_chunk_size = 128 * 1024 * 1024
default_chunk_size_keep_alive = 5 * 1024 * 1024
default_concurrency = 4

def urlopen(url: str, chunk_size: Optional[int]=None, concurrency: Optional[int]=None) -> BaseURLReader:
    if concurrency is None:
        chunk_size = chunk_size or default_chunk_size
        return URLRawReader(url)
    elif 1 == concurrency:
        chunk_size = chunk_size or default_chunk_size_keep_alive
        return URLReaderKeepAlive(url, chunk_size)
    else:
        chunk_size = chunk_size or default_chunk_size
        concurrency = concurrency or default_concurrency
        return URLReader(url, chunk_size, concurrency)

def iter_content(url: str,
                 chunk_size: Optional[int]=None,
                 concurrency: Optional[int]=None) -> Generator[memoryview, None, None]:
    if concurrency is None:
        chunk_size = chunk_size or default_chunk_size
        return URLRawReader.iter_content(url, chunk_size)
    elif 1 == concurrency:
        chunk_size = chunk_size or default_chunk_size_keep_alive
        return URLReaderKeepAlive.iter_content(url, chunk_size)
    else:
        chunk_size = chunk_size or default_chunk_size
        concurrency = concurrency or default_concurrency
        return URLReader.iter_content(url, chunk_size, concurrency)
