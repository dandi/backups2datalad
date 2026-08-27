"""Timing harness. Same workloads for every implementation."""
from pathlib import Path
import sys, time
import anyio
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Sliced, load, read_all


async def timeit(mod, data, chunk, expect):
    t0 = time.monotonic()
    out = await read_all(mod, Sliced(data, chunk))
    dt = time.monotonic() - t0
    assert len(out) == expect, f"expected {expect} lines, got {len(out)}"
    return dt


async def main(path):
    mod = load(path)
    print(f"{path}")
    for mb in (1, 8, 32):
        data = "x" * (mb << 20) + "\n"
        print(f"  one {mb:>2} MiB line   : {await timeit(mod, data, 65536, 1):7.3f} s")
    short = "".join(f'{{"success":true,"key":"k{i:06d}"}}\n' for i in range(200000))
    print(f"  200k short lines : {await timeit(mod, short, 65536, 200000):7.3f} s")
    mixed = "".join(("y" * 200 + "\n") * 5 + "z" * 300000 + "\n" for _ in range(30))
    print(f"  mixed short/long : {await timeit(mod, mixed, 65536, 180):7.3f} s")
    crlf = "".join(f"line{i:06d}\r\n" for i in range(200000))
    print(f"  200k CRLF lines  : {await timeit(mod, crlf, 65536, 200000):7.3f} s")


anyio.run(main, sys.argv[1])
