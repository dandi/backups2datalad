"""Shared helpers: load an implementation module by path, fake streams."""
import importlib.util, sys
import anyio


def load(path):
    spec = importlib.util.spec_from_file_location("impl_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["impl_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


class Chunks:
    """A minimal `str` receive stream yielding the given chunks."""
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.closed = False

    async def receive(self):
        if not self.chunks:
            raise anyio.EndOfStream()
        return self.chunks.pop(0)

    async def aclose(self):
        self.closed = True

    @property
    def extra_attributes(self):
        return {}


class Sliced(Chunks):
    """Yields `data` in fixed-size pieces without materialising them all."""
    def __init__(self, data, size=65536):
        self.data, self.size, self.i = data, size, 0
        self.closed = False

    async def receive(self):
        if self.i >= len(self.data):
            raise anyio.EndOfStream()
        piece = self.data[self.i : self.i + self.size]
        self.i += self.size
        return piece


async def read_all(mod, stream):
    s = mod.LineReceiveStream(stream)
    out = []
    while True:
        try:
            out.append(await s.receive())
        except anyio.EndOfStream:
            return out
