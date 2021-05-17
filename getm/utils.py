import os
from uuid import uuid4
from typing import Optional, Tuple, Union

from getm.http import http
import getm.checksum as checksum


def checksum_for_url(url: str) -> Tuple[Optional[str], Optional[checksum.GETMChecksum]]:
    """Probe headers for checksum information. Return expected checksum value, as well as GETMChecksum instance for
    computing checksum using downloaded data.
    """
    hashes = http.checksums(url)
    cs: Union[None, checksum.MD5, checksum.GSCRC32C, checksum.S3MultiEtag]
    if 'gs_crc32c' in hashes:
        expected_cs = hashes['gs_crc32c']
        cs = checksum.GSCRC32C()
    elif 's3_etag' in hashes:
        expected_cs = hashes['s3_etag']
        size = http.size(url)
        number_of_parts = checksum.part_count_from_s3_etag(expected_cs)
        cs = checksum.S3MultiEtag(size, number_of_parts)
    elif 'md5' in hashes:
        expected_cs = hashes['md5']
        cs = checksum.MD5()
    else:
        expected_cs = cs = None
    return expected_cs, cs

class indirect_open:
    """This should be used as a context manager. Provides a file object to a temporary file. Temporary file is moved to
    'filepath' if no error occurs before close. Attempt to remove temporary file in all cases.
    """
    def __init__(self, filepath: str, tmp: Optional[str]=None):
        self.filepath = filepath
        self.tmp = tmp or f"/tmp/getm-{uuid4()}"

    def __enter__(self):
        self.handle = open(self.tmp, "wb", buffering=0)
        return self.handle

    def __exit__(self, exc_type, exc_value, traceback):
        self.handle.close()
        if exc_type is None:
            if os.path.isfile(self.filepath):
                os.remove(self.filepath)
            os.link(self.tmp, self.filepath)
        os.remove(self.tmp)
