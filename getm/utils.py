import os
import sys
from uuid import uuid4
from typing import Optional, Tuple, Union

from getm.http import http


def resolve_target(url: str, filepath: Optional[str]=None) -> str:
    """
    Resolve the absolute target distination for 'url' given optional 'filepath, creating subdirectories as needed.
    There are three modes of operation:
      1. If 'filepath' is omitted append name derived from 'url' to current working directory.
      2. If 'filepath' is either an existing directory or ends with the system path separate, append 'name' to end of
         filepath.
      3. Otherwise 'filepath' is assumed to include both directory and filename.
    """
    if not filepath:
        filepath = os.path.abspath(http.name(url))
    else:
        filepath = os.path.expanduser(filepath)
        if os.path.isdir(filepath) or filepath.endswith(os.path.sep):
            os.makedirs(filepath, exist_ok=True)
            filepath = os.path.join(os.path.abspath(filepath), http.name(url))
        else:
            filepath = os.path.abspath(filepath)
            dirs = os.path.dirname(filepath)
            os.makedirs(dirs, exist_ok=True)
            filepath = os.path.join(dirs, os.path.basename(filepath))
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

def available_shared_memory() -> int:
    """Return the amount of available shared memory. If this cannot be determined, return '-1'."""
    if "darwin" == sys.platform:
        return -1
    elif "linux" == sys.platform:
        import shutil
        total, used, free = shutil.disk_usage("/dev/shm")
        return free
    else:
        raise RuntimeError("Your system is not supported.")
