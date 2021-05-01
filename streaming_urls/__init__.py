from streaming_urls import config
from streaming_urls.reader import URLRawReader, URLReader, iter_content, iter_content_unordered
from typing import Optional

def urlopen(url: str,
            chunk_size: int=config.default_chunk_size,
            concurrency: Optional[int]=config.default_concurrency):
    if concurrency is None:
        return URLRawReader(url)
    else:
        return URLReader(url, chunk_size, concurrency)
