import os
from uuid import uuid4
from typing import Optional, Tuple, Union

from getm.http import http


def resolve_filepath(url: str, filepath: Optional[str]=None) -> str:
    """
    Return the absolute filepath.
    """
    if not filepath:
        filepath = os.path.abspath(http.name(url))
    else:
        filepath = os.path.abspath(os.path.expanduser(filepath))
        if os.path.isdir(filepath):
            filepath = os.path.join(filepath, http.name(url))
    return filepath

class indirect_open:
    """This should be used as a context manager. Provides a file object to a temporary file. Temporary file is moved to
    'filepath' if no error occurs before close. Attempt to remove temporary file in all cases.
    """
    def __init__(self, filepath: str, tmp: Optional[str]=None):
        assert filepath == os.path.normpath(filepath)
        self.filepath = filepath
        self.tmp = tmp or f"{os.path.dirname(filepath)}/.getm-{uuid4()}"

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
