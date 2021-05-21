"""Provide a consistent interface to checksumming, smoothing out the various heterodoxies of cloud native checksums.
I'm looking at you GS and S3.
"""
import enum
import base64
import hashlib
import binascii
from math import ceil
from typing import List, Optional, Set, Union

import google_crc32c


MB = 1024 * 1024
BytesLike = Union[bytes, bytearray, memoryview]

class _Hasher:
    def __init__(self, data: Optional[bytes]=None):
        pass

    def update(self, data: BytesLike):
        raise NotImplementedError()

    def matches(self, val: str) -> bool:
        raise NotImplementedError()

class MD5(_Hasher):
    def __init__(self, data: Optional[bytes]=None):
        self._checksum = hashlib.md5(data or b"")

    def update(self, data: BytesLike):
        self._checksum.update(data)

    def matches(self, val: str) -> bool:
        return self._checksum.hexdigest() == val

class GSCRC32C(_Hasher):
    def __init__(self, data: Optional[bytes]=None):
        self._checksum = google_crc32c.Checksum(data or b"")

    def update(self, data: BytesLike):
        if isinstance(data, memoryview):
            data = bytes(data)
        self._checksum.update(data)

    def hexdigest(self) -> str:
        return self._checksum.digest().hex()

    def gs_crc32c(self) -> str:
        # Compute the crc32c value assigned to Google Storage objects.
        # kind of wonky, right?
        return base64.b64encode(self._checksum.digest()).decode("utf-8")

    def matches(self, expected_gs_crc32c: str) -> bool:
        return self.gs_crc32c() == expected_gs_crc32c

class S3Etag(_Hasher):
    def __init__(self, part_size: int):
        self.part_size = part_size
        self._etags: List[str] = list()
        self._current_md5 = hashlib.md5()
        self._current_part_size = 0

    def update(self, data: BytesLike):
        while len(data) + self._current_part_size >= self.part_size:
            to_add = self.part_size - self._current_part_size
            self._current_md5.update(data[:to_add])
            self._etags.append(self._current_md5.hexdigest())
            data = data[to_add:]
            self._current_part_size = 0
            self._current_md5 = hashlib.md5()
        self._current_md5.update(data)
        self._current_part_size += len(data)

    def s3_etag(self) -> str:
        if self._current_part_size:
            self._etags.append(self._current_md5.hexdigest())
        if 1 == len(self._etags):
            return self._etags[0]
        else:
            bin_md5 = b"".join([binascii.unhexlify(etag) for etag in self._etags])
            composite_etag = hashlib.md5(bin_md5).hexdigest() + "-" + str(len(self._etags))
            return composite_etag

    def matches(self, val: str) -> bool:
        return self.s3_etag() == val

class S3MultiEtag(_Hasher):
    def __init__(self, size: int, number_of_parts: int):
        part_sizes = _s3_multipart_layouts(size, number_of_parts)
        assert 5 >= len(part_sizes), "Too many possible S3 part layouts!"
        self.etags = [S3Etag(part_size) for part_size in part_sizes]

    def update(self, data: BytesLike):
        for etag in self.etags:
            etag.update(data)

    def s3_etags(self) -> Set[str]:
        return {etag.s3_etag() for etag in self.etags}

    def matches(self, val: str) -> bool:
        return val in self.s3_etags()

class NoopChecksum(_Hasher):
    def update(self, data: BytesLike):
        pass

    def matches(self, val: str) -> bool:
        return True

def _s3_multipart_layouts(size: int, number_of_parts: int) -> List[int]:
    """Compute all possible part sizes for 'number_of_parts'. Part size is assumbed to be multiples of 1 MB."""
    if 1 == number_of_parts:
        return [size]
    assert size >= MB, "Total size less than 1 MB!"
    min_part_size = ceil(size / number_of_parts / MB) * MB
    max_part_size = (ceil(size / (number_of_parts - 1) / MB) - 1) * MB
    if min_part_size == max_part_size:
        part_sizes = [min_part_size]
    else:
        part_sizes = [min_part_size + i * MB for i in range(1 + (max_part_size - min_part_size) // MB)]
    return part_sizes

def part_count_from_s3_etag(s3_etag: str) -> int:
    parts = s3_etag.split("-", 1)
    if 1 == len(parts):
        return 1
    else:
        return int(parts[1])

class Algorithms(enum.Enum):
    md5 = (MD5,)
    gs_crc32c = (GSCRC32C,)
    s3_etag = (S3MultiEtag,)
    null = (NoopChecksum,)

    def __init__(self, checksum_class: type):
        self.cls = checksum_class

class GETMChecksum:
    def __init__(self, expected: str, algorithm: str):
        self.expected = expected
        self.algorithm = Algorithms[algorithm]

    def set_s3_size_and_part_count(self, size: int, part_count: int):
        self._s3_size = size
        self._s3_part_count = part_count

    @property
    def cs(self):
        if not hasattr(self, "_cs"):
            if Algorithms.s3_etag == self.algorithm:
                self._cs = self.algorithm.cls(self._s3_size, self._s3_part_count)
            else:
                self._cs = self.algorithm.cls()
        return self._cs

    def update(self, data: BytesLike):
        self.cs.update(data)

    def matches(self) -> bool:
        return self.cs.matches(self.expected)
