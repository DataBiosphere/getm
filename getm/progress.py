import time
import logging
from math import ceil, floor
from multiprocessing import Lock
from typing import Callable


logger = logging.getLogger(__name__)
ChunkerCallback = Callable[[int, int, int, int, float], None]

class Chunker:
    def __init__(self, size: int, num_chunks: int, callback: ChunkerCallback):
        self.size = size
        self.num_chunks = num_chunks
        self.chunk_size = size / num_chunks
        self.callback = callback
        self.progress = 0
        self.chunks_completed = 0
        self.start = time.time()

    def add(self, sz: int):
        self.progress += sz
        if self.size < self.progress:
            raise ValueError("More than 100% progress!")
        chunks_completed = floor(self.progress / self.chunk_size)
        if chunks_completed > self.chunks_completed:
            self.chunks_completed = chunks_completed
            chunks_remaining = self.num_chunks - chunks_completed
            duration = time.time() - self.start
            self.callback(self.size, self.progress, chunks_completed, chunks_remaining, duration)

    def is_complete(self) -> bool:
        return self.progress >= self.size

class ProgressIndicator:
    def __init__(self, name: str, size: int, incriments: int=40):
        self.name = name
        self._chunker = Chunker(size, incriments, self._print)
        self._downloaded_field_width = len(f"{size}")

    def add(self, sz: int):
        self._chunker.add(sz)

    def _print(self, size: int, progress: int, chunks_completed: int, chunks_remaining: int, duration: float):
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

    def _print(self, size: int, progress: int, chunks_completed: int, chunks_remaining: int, duration: float):
        bar = "{name} {percent:3d}% [{parts}] {downloaded}/{size} {duration:.2f}s".format(
            name=self.name,
            percent=floor(progress / size * 100),
            parts="=" * chunks_completed + " " * chunks_remaining,
            downloaded=f"{{:{self._downloaded_field_width}d}}".format(progress),
            size=size,
            duration=duration
        )
        with self._lock:
            print("\r", bar, end="", flush=True)

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.add(0)
            with self._lock:
                print(flush=True)

class ProgressLogger(ProgressIndicator):
    def _print(self, size, progress, chunks_completed, chunks_remaining, duration):
        bar = "{name} {percent:3d}% {downloaded}/{size} {duration:.2f}s".format(
            name=self.name,
            percent=floor(progress / size * 100),
            downloaded=f"{{:{self._downloaded_field_width}d}}".format(progress),
            size=size,
            duration=duration
        )
        logger.info(bar)
