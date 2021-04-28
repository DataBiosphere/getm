import requests
import warnings
from functools import lru_cache
from urllib.parse import urlparse

from requests import codes
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


default_retry = Retry(total=10,
                      status_forcelist=[429, 500, 502, 503, 504],
                      method_whitelist=["HEAD", "GET"])

class Session(requests.Session):
    def get_range_readinto(self, url: str, start: int, size: int, buf: memoryview):
        for _ in range(10):
            resp = self.get(url, headers=dict(Range=f"bytes={start}-{start + size - 1}"), stream=True)
            resp.raise_for_status()
            # Occasionally an incomplete part is provided with OK status. Check size and retry.
            # Note: Persistent read/readinto on existing connection does not seem to work.
            if size == resp.raw.readinto(buf):
                break
            else:
                msg = ("HTTP range request returned incomplete part. Retrying "
                       f"size={size} "
                       f"content-length={resp.headers['Content-Length']} "
                       f"status-code={resp.status_code}")
                warnings.warn(msg)
        else:
            raise Exception("Failed to download part")

    @lru_cache(maxsize=20)
    def head(self, url: str):
        resp = super().head(url)
        if codes.forbidden == resp.status_code:
            # S3 signed urls return 403 for HEAD, possibly depending on signer.
            resp = self.get(url, stream=True)
            resp.raise_for_status()
        else:
            resp.raise_for_status()
        return resp.headers

    def size(self, url: str) -> int:
        return int(self.head(url)['Content-Length'])

    def raw(self, url: str):
        resp = self.get(url, stream=True)
        resp.raise_for_status()
        return resp.raw

    def iter_content(self, url: str, chunk_size: int):
        resp = self.get(url, stream=True)
        resp.raise_for_status()
        return resp.iter_content(chunk_size=chunk_size)

    def name(self, url: str) -> str:
        """
        Attempt to discover a filename associated with 'url' using the foolowing methods, in order:
        1. Parse the 'Content-Disposition' header for the 'filename' field
        2. Use the last path component of the 'path' field returned from urllib.parse.urlparse
        """
        name = ""
        content_disposition = self.head(url).get('Content-Disposition', "")
        for part in content_disposition.split(";"):
            if part.strip().startswith("filename"):
                name = part.split("=", 1)[-1].strip("'\"")
                break
        name = name or urlparse(url).path.rsplit("/", 1)[-1]
        if name:
            return name
        raise ValueError(f"Unable to extract name from url '{url}'")

    def checksums(self, url: str):
        """
        Extract checksum hashes from headers.

        Checksums for Google Storage:
        The md5 checksum may be missing for some GS objects such as large composite files, however the crc32c checksum
        will always be present. It is safest to always use crc32c, although this method will provide both when present.
        https://cloud.google.com/storage/docs/hashes-etags

        Checksums for AWS S3:
        S3 objects provide an "ETag" header, which is the same as md5 for small objects. For large multipart objects it
        is a more complex object.
        https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html

        Content-MD5:
        https://tools.ietf.org/html/rfc1864
        """
        headers = self.head(url)
        checksums = dict()
        if 'x-goog-hash' in headers:
            for part in headers['x-goog-hash'].split(","):
                name, val = part.strip().split("=", 1)
                if "crc32c" == name:
                    checksums['gs_crc32c'] = val
                if "md5" == name:
                    checksums['gs_md5'] = val
        if 'ETag' in headers:
            etag = headers['ETag'].strip("\"")
            if "AmazonS3" in headers.get('Server', ""):
                checksums['s3_etag'] = etag
            else:
                checksums['etag'] = etag
        if 'Content-MD5' in headers:
            checksums['md5'] = headers['Content-MD5']
        return checksums

def http_session(session: Session=None, retry: Retry=None) -> Session:
    session = session or Session()
    retry = retry or default_retry
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
