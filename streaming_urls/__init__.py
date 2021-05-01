from typing import Generator, Optional, Tuple

from streaming_urls import config
from streaming_urls.reader import BaseURLReader, URLRawReader, URLReader, iter_content_unordered


def urlopen(url: str,
            chunk_size: int=config.default_chunk_size,
            concurrency: Optional[int]=config.default_concurrency) -> BaseURLReader:
    if concurrency is None:
        return URLRawReader(url)
    else:
        return URLReader(url, chunk_size, concurrency)

def iter_content(url: str,
                 chunk_size: int=config.default_chunk_size,
                 concurrency: Optional[int]=config.default_concurrency) -> Generator[memoryview, None, None]:
    if concurrency is None:
        return URLRawReader.iter_content(url, chunk_size)
    else:
        return URLReader.iter_content(url, chunk_size, concurrency)
