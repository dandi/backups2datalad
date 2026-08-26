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

Pass `-c` with the production config so that the run exercises the same code
paths as production -- in particular, `zarrs.remote` being set is what makes
the `whereis` phase run at all.  Without it the Zarr is backed up with no
backup remote, which the current code skips `whereis` for entirely, making the
comparison flattering rather than representative.  If the backup remote can't
be initialised in your test environment (credentials, network), add
`--no-backup-remote`... but then say so when quoting the numbers.

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

## `sync-one-zarr`

The single-Zarr driver that `compare-zarr-backup` invokes.  Calls `sync_zarr()`
directly, since `backups2datalad backup-zarrs` would back up every Zarr in a
Dandiset.  Usable on its own:

```shell
DANDI_API_KEY=... tools/sync-one-zarr -c backups2datalad.cfg.yaml \
    d9d37986-3202-4461-8957-8a0b3fe4b47a /tmp/ab/zarrs/d9d37986-...
```

It prints `ELAPSED_SECONDS <n>` on stdout as its last line.
