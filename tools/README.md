# Development tools

Not part of the installed package; these are helpers for working on
backups2datalad itself.

## `compare-zarr-backup`

Backs up a single Zarr twice -- once with a baseline revision of this repo,
once with the revision under test -- then reports how long each took and
whether the two produced the same repository state.

```shell
git fetch origin
tools/compare-zarr-backup -c /path/to/backups2datalad.cfg.yaml \
    d9d37986-3202-4461-8957-8a0b3fe4b47a
```

Each variant gets its own `git worktree` and virtualenv under the output
directory (`./zarr-ab` by default), so the two runs can't contaminate each
other, and each backup starts from an empty directory.  Nothing is pushed to
GitHub: the GitHub orgs are cleared from the config before the run.

### Nothing deployed is touched

No deployed config is read, no deployed backup root is written to, and nothing
is pushed to GitHub (the GitHub orgs are cleared from the config before the
run).  The only production systems involved are the DANDI API and S3, both
read-only: the run lists the Zarr's object versions to learn its entries.  The
Zarr's content is never downloaded -- `fromkey` and `registerurl` only record
metadata.

`zarrs.remote` being set is what makes the `whereis` phase run at all, so a
run with no backup remote is not representative.  Rather than borrowing the
deployed one, a local `type=directory` remote is stood up inside the output
directory: same code path, no credentials, no deployed storage.  `-c` will use
a real config's remote instead if you want that, and `--no-backup-remote`
drops the remote entirely -- but timings taken that way skip `whereis` and
aren't comparable to production.

`-n` runs each variant twice, which is worth doing: the first run of either
variant pays for cold caches and the S3 listing.

### What "same state" means

Commit SHAs are deliberately *not* compared.  `custom_commit_env()` pins
`GIT_AUTHOR_DATE` but the committer date comes from the wall clock, so two
runs of identical code produce different SHAs.  Most of the git-annex branch
is likewise unreproducible: each `datalad create` mints a fresh repo UUID,
`initremote` mints fresh special-remote UUIDs, and every log line carries a
timestamp.

What the script compares is everything the input actually determines:

- each commit's tree, message, and author identity/date
- the tree of `draft`, and every entry in it
- every path -> annex key mapping, read from the working tree symlinks
- the URLs registered against each key, with timestamps stripped
- which keys are recorded as present in the *web* remote (whose UUID is a
  well-known constant; other remotes' UUIDs vary per run)

A clean run ends with `IDENTICAL`; otherwise the diff is left in
`<outdir>/fingerprint.diff`.  Both datasets also get a `git fsck`.

## `bench-zarr-registration`

An isolated micro-benchmark of just the entry-registration phase -- the part
the batching change actually affects.  Needs nothing but git-annex: no DANDI
API, no S3, no DataLad, no config, no network.  It synthesises N Zarr entries
with made-up digests and URLs and registers them into a throwaway repo, either
`--mode serial` (one entry at a time, the pre-batching path, which works on
both old and new revisions) or `--mode batched`.

```shell
tools/bench-zarr-registration -n 8000 --mode serial  /tmp/bench/serial
tools/bench-zarr-registration -n 8000 --mode batched /tmp/bench/batched
```

`--mode batched` prints a per-phase breakdown, which is the useful part: it
says which git-annex command the time is going to.  Measured on one machine
with git-annex 10.20240129, it is `fromkey` -- around 5.7 ms/entry, roughly
two thirds of the total, flat from n=2,000 to n=20,000 -- and batching cannot
help with that, since it is git-annex's own symlink-and-stage work.

The two modes must produce byte-identical repositories; the `fingerprint`
function in `compare-zarr-backup` will tell you whether they did.

## `sync-one-zarr`

The single-Zarr driver that `compare-zarr-backup` invokes.  Calls `sync_zarr()`
directly, since `backups2datalad backup-zarrs` would back up every Zarr in a
Dandiset.  Usable on its own:

```shell
DANDI_API_KEY=... tools/sync-one-zarr -c backups2datalad.cfg.yaml \
    d9d37986-3202-4461-8957-8a0b3fe4b47a /tmp/ab/zarrs/d9d37986-...
```

It prints `ELAPSED_SECONDS <n>` on stdout as its last line.
