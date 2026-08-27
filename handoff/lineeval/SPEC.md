# Task: replace `linesep` in backups2datalad's stream reading

`backups2datalad` uses the `linesep` package in exactly two places, both in
`src/backups2datalad/aioutil.py`:

1. `LineReceiveStream` — wraps an anyio `str` receive stream (a subprocess's
   stdout via `TextReceiveStream`) and yields one line at a time.  Built on
   `get_newline_splitter(newline, retain=True)`.  Used by `TextProcess`
   (`git annex ... --batch` interaction) and `stream_lines_command()`.
2. `stream_null_command()` — splits NUL-separated records from
   `git ls-tree -z` output, via `TerminatedSplitter("\0", retain=False)`.

## Required behaviour of `LineReceiveStream`

With `newline=None` (the only value used in the codebase), universal newlines:
`\n`, `\r\n` and a lone `\r` each terminate a line, are **translated to `\n`**
in the returned string, and are **retained** (the returned line ends with
`\n`).  A final line with no terminator is returned as-is.  When the transport
is exhausted and nothing is buffered, `receive()` raises `anyio.EndOfStream`.

Observed reference behaviour (chunks in, lines out):

    ["a\nb\n"]        -> ["a\n", "b\n"]
    ["a\r\nb\r\n"]    -> ["a\n", "b\n"]
    ["a\rb\r"]        -> ["a\n", "b\n"]
    ["a\r", "\nb\n"]  -> ["a\n", "b\n"]      # CRLF straddling a chunk
    ["a\r", "b\n"]    -> ["a\n", "b\n"]
    ["no-terminator"] -> ["no-terminator"]
    ["a\n\n\nb"]      -> ["a\n", "\n", "\n", "b"]
    ["xxxxx\r"]       -> ["xxxxx\n"]

The `newline` parameter should still be accepted; when it is a string, that
exact string terminates a line and is retained untranslated.  Nothing in the
codebase passes it, so a clean minimal treatment is acceptable — say what you
chose.

## The problem being solved

`linesep` 0.5.1's `Splitter.feed()` appends to a buffer and re-searches it from
the start, so reading a single line that spans many chunks costs time quadratic
in the line's length.  `git annex whereis --json` for a key with many
registered URLs emits one JSON line of tens of megabytes, arriving in ~33 KiB
reads; reading 32 MiB took ~33 s.  Your implementation must be **linear**.

Note this is fixed upstream in a pending PR, so "keep linesep, wait" is a
legitimate finding — but this exercise is to see what a linesep-free
implementation looks like.

## What to deliver

Write ONE self-contained module at `handoff/lineeval/impl_<YOURSLUG>.py` exposing:

```python
class LineReceiveStream(anyio.abc.ObjectReceiveStream[str]):
    def __init__(self, transport_stream, newline: str | None = None) -> None: ...
    async def receive(self) -> str: ...          # raises anyio.EndOfStream at end
    async def aclose(self) -> None: ...
    @property
    def extra_attributes(self): ...

# Optional but valued: how you would also replace the NUL splitting.
# If you have an answer, expose it as an async generator:
async def iter_null_separated(transport_stream): ...   # yields records, no NULs
```

It must import cleanly and depend only on the standard library, `anyio`, and —
if you genuinely think it is the best answer — ONE additional widely-used,
small, actively-maintained PyPI package (say which, and why it earns its
place).  You may `pip install` to try things.

## Judging

Run these yourself before reporting; I will run them again:

    python3 handoff/lineeval/conformance.py handoff/lineeval/impl_<YOURSLUG>.py
    python3 handoff/lineeval/benchmark.py   handoff/lineeval/impl_<YOURSLUG>.py

Conformance is a randomised differential test against `linesep` itself.
Anything less than a clean pass is a failed submission.

You are judged on, in order: **correctness**, **clarity/simplicity** (a
reviewer should be able to convince themselves it is right — line count and
number of state variables both matter), and **performance**.

## Report back

- the approach in 2-3 sentences, and why you rejected the alternatives you considered
- your conformance and benchmark output
- honest weaknesses of your solution
- your recommendation: is this better than keeping `linesep` (with the upstream
  fix landing soon)?  Say so plainly either way.
