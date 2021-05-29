import time
import logging
from math import ceil, floor
from multiprocessing import Lock
from typing import Callable


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ChunkerCallback = Callable[[int, int, float], None]

def sizeof_fmt(num, suffix='B'):
    assert 0 <= num
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if num < 1024:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024
    return "%.1f%s%s" % (num, 'Yi', suffix)

class Chunker:
    def __init__(self, size: int, chunk_size: float, callback: ChunkerCallback):
        self.size = size
        self.chunk_size = chunk_size
        self.num_chunks = ceil(size / chunk_size)
        self.callback = callback
        self.progress = 0
        self.chunks_completed = 0
        self.start = time.time()

    def add(self, sz: int):
        self.progress += sz
        if self.size < self.progress:
            raise ValueError("More than 100% progress!")
        chunks_completed = self.num_chunks if self.size == self.progress else floor(self.progress / self.chunk_size)
        if chunks_completed > self.chunks_completed:
            self.chunks_completed = chunks_completed
            duration = time.time() - self.start
            self.callback(self.size, self.progress, duration)

    def is_complete(self) -> bool:
        return self.progress >= self.size

class ProgressIndicator:
    chunker_chunk_size = 1024 * 1024 * 8.0

    def __init__(self, name: str, size: int, incriments: int=40):
        self.name = name
        self._chunker = Chunker(size, self.chunker_chunk_size, self._print)
        self._downloaded_field_width = len(f"{size}")

    def add(self, sz: int):
        self._chunker.add(sz)

    def _print(self, size: int, progress: int, duration: float):
        raise NotImplementedError()

    def __enter__(self):
        self.add(0)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.add(0)

class ProgressBar(ProgressIndicator):
    def __init__(self, name: str, size: int, incriments: int=40):
        super().__init__(name, size, incriments)
        self._lock = Lock()
        self.name = self.name[:40]
        self._chunk_size = size / 40

    def _print(self, size: int, progress: int, duration: float):
        if progress == size:
            chunks_completed = ceil(progress / self._chunk_size)
            chunks_remaining = 0
        else:
            chunks_completed = floor(progress / self._chunk_size)
            chunks_remaining = ceil(size / self._chunk_size) - chunks_completed
        bar = "{name}   {percent:3d}%   [{parts}]   {size}   {rate}/s   {duration:.2f}s".format(
            name=self.name,
            percent=floor(progress / size * 100),
            parts="=" * chunks_completed + " " * chunks_remaining,
            size=sizeof_fmt(size),
            rate=sizeof_fmt(progress / duration),  # integrated rate
            duration=duration,
        )
        with self._lock:
            print("\r", bar, end="", flush=True)

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.add(0)
            with self._lock:
                print(flush=True)

class ProgressLogger(ProgressIndicator):
    def __init__(self, name: str, size: int, incriments: int=40):
        self.chunker_chunk_size = size / 40
        super().__init__(name, size, incriments)

    def _print(self, size, progress, duration):
        bar = "{name} {percent:3d}% {size} {rate}/s {duration:.6f}s".format(
            name=self.name,
            percent=floor(progress / size * 100),
            size=sizeof_fmt(size),
            rate=sizeof_fmt(progress / duration),  # integrated rate
            duration=duration
        )
        logger.info(bar)
