"""Extra differential tests: non-default newline values, NUL records, real pipes."""
from pathlib import Path
import random, sys
import anyio
from linesep import TerminatedSplitter, get_newline_splitter
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _harness import Chunks, load, read_all
mod = load(str(Path(__file__).resolve().parent / "impl_stdlib.py"))

def oracle(sp, chunks):
    out = []
    for c in chunks:
        sp.feed(c); out.extend(sp.getall())
    sp.close(); out.extend(sp.getall())
    return out

async def read_all_nl(stream, newline):
    s = mod.LineReceiveStream(stream, newline)
    out = []
    while True:
        try: out.append(await s.receive())
        except anyio.EndOfStream: return out

async def main():
    rnd = random.Random(1234)
    bad = 0
    ALPHA = ["a", "b", "\n", "\r", "\r\n", "", "zz", "\0", "\n\n", "\r\r"]
    for trial in range(6000):
        text = "".join(rnd.choice(ALPHA) for _ in range(rnd.randint(0, 14)))
        chunks, i = [], 0
        while i < len(text):
            j = min(len(text), i + rnd.randint(1, 5)); chunks.append(text[i:j]); i = j
        for nl in ("\n", "\r", "\r\n", "\0"):
            want = oracle(TerminatedSplitter(nl, retain=True), chunks)
            got = await read_all_nl(Chunks(chunks), nl)
            if want != got:
                bad += 1
                if bad < 6: print(f"  newline={nl!r} {chunks!r}: linesep {want!r} got {got!r}")
        # iter_null_separated vs TerminatedSplitter("\0", retain=False)
        want = oracle(TerminatedSplitter("\0", retain=False), chunks)
        got = [r async for r in mod.iter_null_separated(Chunks(chunks))]
        if want != got:
            bad += 1
            if bad < 6: print(f"  NUL {chunks!r}: linesep {want!r} got {got!r}")
    print("differential (newline=\\n,\\r,\\r\\n,\\0 + iter_null_separated), failures:", bad)

    # rejected newline values
    for nl in ("", "END", "\n\n"):
        try:
            mod.LineReceiveStream(Chunks([]), nl); print("  MISSING ValueError for", repr(nl)); bad += 1
        except ValueError: pass

    # real subprocesses through anyio + TextReceiveStream
    from anyio.streams.text import TextReceiveStream
    p = await anyio.open_process(["python3", "-c",
        r"import sys;sys.stdout.write('a\r\n'+'b'*200000+'\r'+'\xe9\n'+'tail')"])
    assert p.stdout is not None
    lines = [ln async for ln in mod.LineReceiveStream(TextReceiveStream(p.stdout))]
    await p.wait()
    ok = lines == ["a\n", "b"*200000 + "\n", "\xe9\n", "tail"]
    print("  subprocess universal-newline lines:", "OK" if ok else f"FAIL {lines[:1]}")
    bad += not ok

    p = await anyio.open_process(["python3", "-c", r"import sys;sys.stdout.write('x\0yy\0z\xe9\0')"])
    assert p.stdout is not None
    recs = [r async for r in mod.iter_null_separated(TextReceiveStream(p.stdout))]
    await p.wait()
    ok = recs == ["x", "yy", "z\xe9"]
    print("  subprocess NUL records:", "OK" if ok else f"FAIL {recs!r}")
    bad += not ok

    # multi-byte UTF-8 char split across two reads must not confuse anything
    ok = await read_all(mod, Chunks(["café\n"])) == ["café\n"]
    print("  non-ascii:", "OK" if ok else "FAIL")
    print("TOTAL FAILURES:", bad)
    return bad

sys.exit(1 if anyio.run(main) else 0)
