import base64
import typing
import binascii
import hashlib
from math import ceil
from typing import Tuple, List, Set, Optional

import google_crc32c


MB = 1024 * 1024

class GSCRC32C:
    def __init__(self, data: Optional[bytes]=None):
        if data is not None:
            self._checksum = google_crc32c.Checksum(data)
        else:
            self._checksum = google_crc32c.Checksum(b"")

    def update(self, data: bytes):
        self._checksum.update(data)

    def hexdigest(self) -> str:
        return self._checksum.digest().hex()

    def strdigest(self) -> str:
        # Compute the crc32c value assigned to Google Storage objects.
        # kind of wonky, right?
        return base64.b64encode(self._checksum.digest()).decode("utf-8")

    def matches(self, val: str) -> bool:
        return self.strdigest() == val

class S3Etag:
    def __init__(self, part_size: int):
        self.part_size = part_size
        self._etags: List[str] = list()
        self._current_md5 = hashlib.md5()
        self._current_part_size = 0

    def update(self, data: bytes):
        while len(data) + self._current_part_size >= self.part_size:
            to_add = self.part_size - self._current_part_size
            self._current_md5.update(data[:to_add])
            self._etags.append(self._current_md5.hexdigest())
            data = data[to_add:]
            self._current_part_size = 0
            self._current_md5 = hashlib.md5()
        self._current_md5.update(data)
        self._current_part_size += len(data)

    def strdigest(self) -> str:
        if self._current_part_size:
            self._etags.append(self._current_md5.hexdigest())
        if 1 == len(self._etags):
            return self._etags[0]
        else:
            bin_md5 = b"".join([binascii.unhexlify(etag) for etag in self._etags])
            composite_etag = hashlib.md5(bin_md5).hexdigest() + "-" + str(len(self._etags))
            return composite_etag

    def matches(self, val: str) -> bool:
        return self.strdigest() == val

class S3MultiEtag:
    def __init__(self, size: int, number_of_parts: int):
        part_sizes = _s3_multipart_layouts(size, number_of_parts)
        assert 5 >= len(part_sizes), "Too many possible S3 part layouts!"
        self.etags = [S3Etag(part_size) for part_size in part_sizes]

    def update(self, data: bytes):
        for etag in self.etags:
            etag.update(data)

    def strdigests(self) -> List[str]:
        return [etag.strdigest() for etag in self.etags]

    def matches(self, val: str) -> bool:
        return val in self.strdigests()

def _s3_multipart_layouts(size: int, number_of_parts: int) -> List[int]:
    """
    Compute all possible part sizes for 'number_of_parts'. Part size is assumbed to be multiples of 1 MB.
    """
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
