import os
from typing import Generator, Optional

from getm import reader, checksum


default_chunk_size = 128 * 1024 * 1024
default_chunk_size_keep_alive = 1 * 1024 * 1024
default_concurrency = 4

def urlopen(url: str, chunk_size: Optional[int]=None, concurrency: Optional[int]=None) -> reader.BaseURLReader:
    if concurrency is None:
        chunk_size = chunk_size or default_chunk_size
        return reader.URLRawReader(url)
    elif 1 == concurrency:
        chunk_size = chunk_size or default_chunk_size_keep_alive
        return reader.URLReaderKeepAlive(url, chunk_size)
    else:
        chunk_size = chunk_size or default_chunk_size
        concurrency = concurrency or default_concurrency
        return reader.URLReader(url, chunk_size, concurrency)

def iter_content(url: str,
                 chunk_size: Optional[int]=None,
                 concurrency: Optional[int]=None) -> Generator[memoryview, None, None]:
    if concurrency is None:
        chunk_size = chunk_size or default_chunk_size
        return reader.URLRawReader.iter_content(url, chunk_size)
    elif 1 == concurrency:
        chunk_size = chunk_size or default_chunk_size_keep_alive
        return reader.URLReaderKeepAlive.iter_content(url, chunk_size)
    else:
        chunk_size = chunk_size or default_chunk_size
        concurrency = concurrency or default_concurrency
        return reader.URLReader.iter_content(url, chunk_size, concurrency)

def download_iter_parts(url: str,
                        filepath: str,
                        chunk_size: Optional[int]=None,
                        concurrency: Optional[int]=1) -> Generator[int, None, None]:
    expected_cs, cs = _get_checksums(url)
    sz = reader.http.size(url)
    if not chunk_size:
        chunk_size = default_chunk_size if 1 != concurrency else default_chunk_size_keep_alive
    try:
        fd = os.open(filepath, os.O_WRONLY | os.O_CREAT)
        os.pwrite(fd, b"0", sz - 1)
        offset = 0
        for part in iter_content(url, chunk_size, concurrency):
            try:
                if expected_cs:
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

def _get_checksums(url):
    hashes = reader.http.checksums(url)
    if 'gs_crc32c' in hashes:
        expected_cs = hashes['gs_crc32c']
        cs = checksum.GSCRC32C()
    elif 's3_etag' in hashes:
        expected_cs = hashes['s3_etag']
        size = reader.http.size(url)
        number_of_parts = checksum.part_count_from_s3_etag(expected_cs)
        cs = checksum.S3MultiEtag(size, number_of_parts)
    elif 'md5' in hashes:
        expected_cs = hashes['md5']
        cs = checksum.hashlib.md5()
    else:
        expected_cs = cs = None
    return expected_cs, cs
