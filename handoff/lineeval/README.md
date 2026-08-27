# `LineReceiveStream` replacement candidates

Working material from the investigation into the quadratic line reading in
`backups2datalad.aioutil.LineReceiveStream` (see dandi/backups2datalad#110,
closed in favour of the upstream fix in jwodder/linesep#54).

This branch exists purely as a handoff; **nothing here is wired into
`backups2datalad`**, and `src/` is untouched.

## Why this is here

`LineReceiveStream` fed each chunk to `linesep`, whose `feed()` re-scans the
whole accumulated buffer, so reading one very long line cost time quadratic in
its length.  That is fixed properly upstream.  Separately, five independent
implementations of "split an anyio `str` stream into lines, without linesep"
were written to a common spec and judged.  `impl_stdlib.py` won.

## `impl_stdlib.py`

Delegates the entire newline problem to `io.IncrementalNewlineDecoder`, the
object `io.TextIOWrapper` uses for universal newlines.  The key property: it
holds back a `"\r"` that ends a chunk until it can see whether the next chunk
starts with `"\n"`.  So no supported terminator can straddle a chunk boundary,
each decoded chunk is split with plain `str.split` and never looked at again,
and nothing is re-scanned.

It also provides `iter_null_separated()`, covering what `stream_null_command()`
uses `TerminatedSplitter("\0")` for.

## Results

Judged on clarity, simplicity and performance.  Times are the minimum of 3
interleaved reps (seconds, lower is better):

| impl | 1 MiB | 8 MiB | 32 MiB | 200k short | 200k CRLF | mixed | code lines | attrs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `linesep` 0.5.1 (baseline) | 0.081 | 3.638 | 62.6 | 0.530 | 0.517 | 0.190 | 27 | 2 |
| `linesep` + upstream fix | 0.009 | 0.074 | 0.338 | 0.544 | 0.517 | 0.059 | 27 | 2 |
| **`impl_stdlib`** | **0.001** | **0.017** | **0.070** | **0.053** | **0.055** | **0.005** | 63 | 8 |
| `impl_fast` | 0.001 | 0.015 | 0.105 | 0.048 | 0.052 | 0.005 | 82 | 8 |
| `impl_library` | 0.001 | 0.014 | 0.058 | 0.087 | 0.087 | 0.002 | 66 | 7 |
| `impl_rethink` | 0.001 | 0.015 | 0.059 | 0.104 | 0.105 | 0.002 | 92 | 7 |
| `impl_minimal` | 0.002 | 0.026 | 0.105 | 0.088 | 0.089 | 0.008 | 63 | 2 |

All five pass conformance.

**The standing recommendation is still to wait for `linesep`, not to adopt
this.** Measured end-to-end through a real pipe, where pipe I/O and UTF-8
decoding dominate, `impl_stdlib` came out at 0.40 s against the fixed
`linesep`'s 0.52 s — about 1.3x, for +36 lines of code this repo would own and
maintain.  The table above flatters the in-process case.  Keep this branch as
the record of what the alternative looks like, in case that trade changes.

## Running it

Requires `anyio` and `linesep` (the latter only as the differential oracle).

    python handoff/lineeval/conformance.py handoff/lineeval/impl_stdlib.py
    python handoff/lineeval/benchmark.py   handoff/lineeval/impl_stdlib.py
    python handoff/lineeval/extra_stdlib.py   # real subprocess pipes, non-ascii
    python handoff/lineeval/judge.py          # interleaved comparison of all impls

`conformance.py` runs 10 enumerated universal-newline cases plus 4000 randomised
chunkings differentially against `linesep`.  `SPEC.md` is the brief the five
implementations were written to.
