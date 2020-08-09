import struct
from multiprocessing import shared_memory
from typing import ByteString, Optional, Tuple


class SharedCircularBuffer:
    def __init__(self, name: Optional[str]=None, size: int=0, create=False):
        if create is True:
            self._shared_memory = shared_memory.SharedMemory(create=True, size=size)
            self._did_create = True
        else:
            self._shared_memory = shared_memory.SharedMemory(name)
        self._view = self._shared_memory.buf

    @property
    def size(self):
        return self._shared_memory.size

    @property
    def name(self):
        return self._shared_memory.name

    def _circular_coords(self, slc: slice) -> Tuple[int, int, bool]:
        if self.size < slc.stop - slc.start:
            raise ValueError("Not enough space in buffer")
        start = slc.start % self.size
        stop = slc.stop % self.size
        wraps = stop <= start or (slc.start != slc.stop and start == stop)
        return start, stop, wraps

    def __getitem__(self, slc: slice) -> memoryview:
        if slc.start == slc.stop:
            raise ValueError("zero length slice not allowed")
        start, stop, wraps = self._circular_coords(slc)
        if wraps:
            return self._view[start:]
        else:
            return self._view[start:stop]

    def __setitem__(self, slc: slice, data: bytes):
        start, stop, wraps = self._circular_coords(slc)
        if wraps:
            wrap_length = self.size - start
            self._view[start:] = data[:wrap_length]  # type: ignore
            self._view[:len(data) - wrap_length] = data[wrap_length:]  # type: ignore
        else:
            self._view[start:stop] = data  # type: ignore

    def close(self):
        if self._shared_memory is not None:
            sm, self._shared_memory = self._shared_memory, None
            sm.close()
            if getattr(self, "_did_create", False):
                sm.unlink()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

STRIDE_FMT = "@LL"
STRIDE_SZ = struct.calcsize(STRIDE_FMT)

class SharedBufferArray:
    def __init__(self, name: Optional[str]=None, chunk_size: int=0, num_chunks: int=0, create=False):
        if create is True:
            self._shared_memory = shared_memory.SharedMemory(create=True, size=(chunk_size * num_chunks) + STRIDE_SZ)
            self._set_stride_info(chunk_size, num_chunks)
            self._did_create = True
        else:
            self._shared_memory = shared_memory.SharedMemory(name)
            self._get_stride_info()

    def _set_stride_info(self, chunk_size: int, num_chunks: int):
        self._shared_memory.buf[-STRIDE_SZ:] = struct.pack(STRIDE_FMT, chunk_size, num_chunks)  # type: ignore
        self.chunk_size, self.num_chunks = chunk_size, num_chunks

    def _get_stride_info(self):
        self.chunk_size, self.num_chunks = struct.unpack(STRIDE_FMT, self._shared_memory.buf[-STRIDE_SZ:])

    @property
    def size(self):
        return self._shared_memory.size - STRIDE_SZ

    @property
    def name(self):
        return self._shared_memory.name

    def __getitem__(self, i: int) -> memoryview:
        if i < self.num_chunks:
            return self._shared_memory.buf[i * self.chunk_size: (i + 1) * self.chunk_size]
        else:
            raise IndexError()

    def close(self):
        if self._shared_memory is not None:
            sm, self._shared_memory = self._shared_memory, None
            sm.close()
            if getattr(self, "_did_create", False):
                sm.unlink()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()
