"""Randomised differential test of an implementation against `linesep`."""
from pathlib import Path
import random, sys
import anyio
from linesep import get_newline_splitter
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Chunks, load, read_all

ALPHABET = ["a", "b", "\n", "\r", "\r\n", "", "zz", "\n\n", "c" * 40]


def oracle(chunks):
    sp = get_newline_splitter(None, retain=True)
    out = []
    for c in chunks:
        sp.feed(c)
        out.extend(sp.getall())
    sp.close()
    out.extend(sp.getall())
    return out


ENUMERATED = [
    (["a\nb\n"], ["a\n", "b\n"]),
    (["a\r\nb\r\n"], ["a\n", "b\n"]),
    (["a\rb\r"], ["a\n", "b\n"]),
    (["a\r", "\nb\n"], ["a\n", "b\n"]),
    (["a\r", "b\n"], ["a\n", "b\n"]),
    (["no-terminator"], ["no-terminator"]),
    (["a\n\n\nb"], ["a\n", "\n", "\n", "b"]),
    (["xxxxx\r"], ["xxxxx\n"]),
    ([], []),
    (["", "", "a\n", ""], ["a\n"]),
]


async def main(path):
    mod = load(path)
    bad = 0
    for chunks, expected in ENUMERATED:
        got = await read_all(mod, Chunks(chunks))
        if got != expected:
            bad += 1
            print(f"  ENUMERATED FAIL {chunks!r}: expected {expected!r} got {got!r}")
    rnd = random.Random(0xC0FFEE)
    trials = 4000
    for _ in range(trials):
        text = "".join(rnd.choice(ALPHABET) for _ in range(rnd.randint(0, 16)))
        chunks, i = [], 0
        while i < len(text):
            j = min(len(text), i + rnd.randint(1, 6))
            chunks.append(text[i:j])
            i = j
        want, got = oracle(chunks), await read_all(mod, Chunks(chunks))
        if want != got:
            bad += 1
            if bad <= 5:
                print(f"  DIFF FAIL {chunks!r}: linesep {want!r} got {got!r}")
    # aclose must propagate
    st = Chunks(["x\n"])
    s = mod.LineReceiveStream(st)
    await s.aclose()
    if not st.closed:
        bad += 1
        print("  aclose() did not close the transport")
    print(f"{path}: {len(ENUMERATED)} enumerated + {trials} randomised, failures: {bad}")
    return bad


sys.exit(1 if anyio.run(main, sys.argv[1]) else 0)
