from typing import Generator, Optional, Tuple

from streaming_urls import config
from streaming_urls.reader import (BaseURLReader, URLRawReader, URLReader, for_each_part, for_each_part_raw,
                                   for_each_part_unordered)


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
        return for_each_part_raw(url, chunk_size)
    else:
        return for_each_part(url, chunk_size, concurrency)

def iter_content_unordered(url: str,
                           chunk_size: int=config.default_chunk_size,
                           concurrency: int=config.default_concurrency) -> Generator[Tuple[int, memoryview], None, None]:  # noqa
    return for_each_part_unordered(url, chunk_size, concurrency)
