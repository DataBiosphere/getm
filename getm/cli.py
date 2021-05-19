import os
import sys

from getm import iter_content
from getm.http import http
from getm.utils import checksum_for_url, indirect_open
from getm.progress import ProgressBar


def do_download(url, name, filepath):
    expected_cs, cs = checksum_for_url(url)
    assert expected_cs is not None, "No checksum information available!"
    with ProgressBar(name, http.size(url)) as progress:
        with indirect_open(filepath) as handle:
            for part in iter_content(url, concurrency=1):
                handle.write(part)
                cs.update(part)
                progress.add(len(part))
            assert cs.matches(expected_cs), "Checksum failed!"

def main():
    url = sys.argv[1]
    name = sys.argv[2]
    filepath = os.path.realpath(sys.argv[2])
    do_download(url, name, filepath)
