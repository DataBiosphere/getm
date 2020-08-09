import requests
import warnings
from functools import lru_cache

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

def http_session(session: Session=None, retry: Retry=None) -> Session:
    session = session or Session()
    retry = retry or default_retry
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
