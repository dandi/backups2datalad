"""Interleaved benchmark: all implementations, N reps, report the minimum."""
from pathlib import Path
import sys, time
import anyio
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Sliced, load

IMPLS = ["impl_baseline_linesep", "impl_minimal", "impl_fast",
         "impl_stdlib", "impl_library", "impl_rethink"]
REPS = 3


def workloads():
    yield "1 MiB line", "x" * (1 << 20) + "\n", 1
    yield "8 MiB line", "x" * (8 << 20) + "\n", 1
    yield "32 MiB line", "x" * (32 << 20) + "\n", 1
    yield ("200k short", "".join(f'{{"success":true,"key":"k{i:06d}"}}\n'
                                 for i in range(200000)), 200000)
    yield ("200k CRLF", "".join(f"line{i:06d}\r\n" for i in range(200000)), 200000)
    yield ("mixed", "".join(("y" * 200 + "\n") * 5 + "z" * 300000 + "\n"
                            for _ in range(30)), 180)


async def once(mod, data, expect):
    s = mod.LineReceiveStream(Sliced(data, 65536))
    n = 0
    t0 = time.monotonic()
    while True:
        try:
            await s.receive()
        except anyio.EndOfStream:
            break
        n += 1
    dt = time.monotonic() - t0
    assert n == expect, f"{n} != {expect}"
    return dt


async def main():
    mods = {name: load(str(Path(__file__).resolve().parent / f"{name}.py")) for name in IMPLS}
    results = {n: {} for n in IMPLS}
    for label, data, expect in workloads():
        for rep in range(REPS):
            for name in IMPLS:
                dt = await once(mods[name], data, expect)
                prev = results[name].get(label)
                results[name][label] = dt if prev is None else min(prev, dt)
        del data
    labels = [w[0] for w in workloads()]
    print(f"{'implementation':24}" + "".join(f"{l:>13}" for l in labels))
    for name in IMPLS:
        row = "".join(f"{results[name][l]:>12.3f}s" for l in labels)
        print(f"{name:24}{row}")


anyio.run(main)
