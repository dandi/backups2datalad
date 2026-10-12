# Plan: `--mode verify` failures of the 2026-10-09 and 2026-10-10 runs

Status: **v1 — proposal** (2026-10-12).  Nothing here is implemented yet.

Logs reviewed (both `update-from-backup --workers 5 --mode verify -e
'00(1412|1411)$'`, backups2datalad `0.20260921.1.dev56+g3defbea09`, i.e.
`main` at `3defbea`):

* `2026.10.09.23.30.21Z.log` — "Backups for 3 Dandisets failed"
* `2026.10.10.23.20.51Z.log` — "Backups for 5 Dandisets failed"

Line numbers `L…` below refer to those log files; `file:line` to `main` at
`3defbea`.

## 1. Summary

| Dandiset | Run(s) | Error | Root cause | Ours? | Existing issue |
| --- | --- | --- | --- | --- | --- |
| 000108 — Zarrs `2d89a8f1…`, `cfb88b49…` | both | `entry '0/0/0/0/0/0' added, but Dandiset draft timestamp was not updated` | Zarr mirror lacks an entry that predates its last sync, and timestamp-mode sync never re-adds such an entry (§2) | **yes** | #66, #90 (item 2) |
| 001246 — Zarr `1c0e023e…` | both | `entry '.zattrs' added …` | same; failing since Feb 2025 | **yes** | #66, #90 |
| 001769 — Zarr `61133c51…` | 2nd | `entry '0/c/0/0/19' added …` | same; the 1st run synced this Zarr in non-verify mode and **still** left the entry missing | **yes** | — |
| 001602 | both | 7 asset metadata diffs: `access` EmbargoedAccess → OpenAccess, draft `modified` unchanged | the archive rewrote asset metadata without bumping `version.modified` (§3) | no (archive) | #49 (closed), dandi-archive#2002 |
| 000571 | 2nd | `still dirty after committing` | 177,024 per-file `git rm` calls (one per *new* file) racing git-annex's own index writes; 15.5 h of serialized index rewrites (§4) | **yes** | #72, #62, #42 |

Also: the final `RuntimeError: Backups for N Dandisets failed` names none of
them — dandi/backups2datalad#146 (open) fixes that.

## 2. Zarr entries that predate the last sync are never re-added (000108, 001246, 001769)

### 2.1 Evidence

The 1st run synced 001769 in non-verify mode (its draft had changed: "1 file
added").  For Zarr `61133c51…` the log shows:

```
L688422 … Zarr 61133c51…: 0/c/0/0/19 on server but not in backup
L688423 … Zarr 61133c51…: sync needed
L688556 … Zarr 61133c51…: 0/c/0/0/19: file not modified since last backup
L691490 … Zarr 61133c51…: no changes; not committing
```

`needs_sync()` sees the entry is missing, asks for a sync, and the sync
loop then skips that very entry because its S3 `LastModified` is older than
the recorded last sync.  The next day, with 001769's draft unchanged, verify
reports the same missing entry as a server-side change.

### 2.2 Code

`ZarrSyncer.run()` (`zarr.py:178-189`), timestamp mode:

```python
to_delete.discard(str(entry))
if self.mode is ZarrMode.TIMESTAMP:
    if last_sync is not None and entry.last_modified < last_sync:
        self.log.debug("%s: file not modified since last backup", entry)
        continue
```

It never checks that the entry is present locally.  An entry missing from the
mirror whose `LastModified` is before `.dandi/s3sync.json`'s `last_modified`
is therefore skipped on every visit, forever.  Two things that would
otherwise catch it don't:

* the checksum guard compares `final_checksum` to the server's, but
  `final_checksum` comes from `zcc`, which is built from the **server
  listing**, skipped entries included (`zarr.py:179`, `:274`), not from local
  content.  So `.dandi/zarr-checksum` records the server checksum even though
  the mirror is missing a file;
* `--mode force` changes only how the *Dandiset* is processed; the Zarr stays
  in timestamp mode, so it skips the entry too.  Only `--zarr-mode
  checksum|force` would repair it.

So these Zarrs stay broken: every verify run raises, and a non-verify run
leaves them as they are.

### 2.3 How entries go missing (hypothesis)

An S3 listing is paginated and not a snapshot.  If a Zarr is being written
while we list it (a re-upload into an existing Zarr does not bump the Dandiset
`modified` until the asset is updated at the end), an object written behind
the listing cursor is missed, while objects written later at keys ahead of the
cursor are seen.  `last_timestamp` is the max `LastModified` seen
(`zarr.py:461`), so it ends up later than the missed object.  This matches
#66's numbers (`.zattrs` `LastModified` 19:27:41 < recorded 19:28:10).
Another known source is a partial commit followed by a manual `git reset
--hard` (#66 comments).  F1a fixes both, whatever the cause.

### 2.4 Fixes

* **F1a (must)**: in timestamp mode, skip an entry only if it is present
  locally.  Snapshot the local paths before the loop, since `to_delete` is
  mutated:

  ```python
  local_paths = frozenset(to_delete)  # after zarr.py:156
  ...
  if (
      last_sync is not None
      and entry.last_modified < last_sync
      and str(entry) in local_paths
  ):
      continue
  ```

  On a `MAX_ZARR_SYNCS` re-pass, entries added in the first pass aren't in
  `local_paths`, so they fall through to the existing hash comparison, which
  is a `readlink` that changes nothing.
* **F1b (should)**: don't advance the watermark past a known-incomplete sync.
  When `changed_during_sync` and the checksum mismatches (`zarr.py:286-293`,
  only a WARNING today), write the *previous* `last_modified` to
  `s3sync.json`, and keep the commit date as is.  The next visit then
  re-examines everything changed since the last sync that is known to be
  good.  It is then a non-verify visit, because the Dandiset's `modified`
  moved.
* **F1c (should)**: make the verify message say what is actually wrong.  In
  `needs_sync()`'s `KeyError` branch, when `obj["LastModified"] <=
  last_sync`, the server did not change after our sync.  The mirror is
  incomplete, so raise e.g. "entry X (S3 LastModified T) is missing from the
  backup although it predates the last sync at L; repair with
  `update-from-backup --mode force <id>`" instead of blaming the archive's
  timestamp.  *Decision*: keep raising in verify (recommended; verify reports
  and does not repair), or repair in place.
* Test (`test_zarr.py`, `@pytest.mark.ai_generated`): back up a Zarr, `git rm`
  one entry from the Zarr mirror and commit (keeping `s3sync.json`), re-sync
  in timestamp mode, and assert the entry is back.  With
  `error_on_change=True`, assert the F1c message.

### 2.5 Remediation

* After F1a is deployed: `tools/backups2datalad-update-cron --mode force
  000108 001246 001769`.
* Today, without code: `--mode force --zarr-mode checksum 000108 001246
  001769`.  The stored checksum (from the server) differs from the local one,
  so these Zarrs sync, and the loop isn't timestamp-gated there.  The cost is
  one local checksum computation per Zarr of these Dandisets.
* Verify's `needs_sync()` lists every Zarr, so only these 4 Zarrs are known to
  be affected.  Entries *overwritten* in place before the last sync are
  invisible to timestamp mode, though, and 001412/001411 are excluded from
  cron.  A one-off `--zarr-mode checksum` sweep would bound both.
* Afterwards, revisit `git diff --exit-code` in
  `dandisets/tools/backups2datalad-update-cron`, which is disabled "since
  000108 is problematic".

## 3. Asset `access` rewritten without bumping `modified` (001602)

### 3.1 Evidence

Both runs: 7 sidecars `sourcedata/raw/sub-I74/micr/*_XPCT.json`
(`blobDateModified` 2025-08-12).  The only diff hunk is

```
   access:
-  - embargoedUntil: '2028-03-10'
-    schemaKey: AccessRequirements
-    status: dandi:EmbargoedAccess
+  - schemaKey: AccessRequirements
+    status: dandi:OpenAccess
```

The draft `modified` is unchanged and `dandiset.yaml` is unchanged
(L1476239).

### 3.2 Diagnosis

The archive changed asset metadata (an embargo cleanup, either a migration or
an admin action) without bumping `version.modified`.  This is the same class
as #49 / dandi-archive#2002 / #2050.  The mirror is correct and verify is
doing its job.  The API is not reachable from the sandbox this was written in,
so the archive side is unconfirmed.

### 3.3 Actions

* Ops: `tools/backups2datalad-update-cron --mode force 001602`, once.
* Upstream: report the 7 asset paths and dates to dandi-archive#2002 (or a
  new issue).  Ask what changed them, and that such fixes bump `modified`.
* Code: none for now.  If such cleanups keep recurring, consider a verify
  allowlist for `access`-only diffs on an OPEN Dandiset, in the spirit of
  `strip_s3_region()`.

## 4. `git rm` per new file vs. git-annex's index writes (000571)

### 4.1 Evidence (2nd run)

* 000571 got 177,011 new files: 172,261 registered via `fromkey`, 4,753 text
  files downloaded via `addurl`.  000571 is mid mass-upload: the 1st run
  skipped it because its listing went stale (#127 path).
* 177,024 `git rm -f --ignore-unmatch` calls, from 22:37:29 to 14:03:49:
  **15.5 h, ~0.31 s each**, serialized under `AsyncDataset.lock`.  The whole
  run took 19 h.
* 10 of them failed on `.git/index.lock`, and `fuser` shows another `git` (PID
  2013037) holding it (L743764…).  That is git-annex staging its own batch
  work; all our retries eventually succeeded.
* `addurl`, `fromkey` and `registerurl` all exited 0 (L2442270-2442276).  The
  commit "[backups2datalad] 177011 files added, 3 files updated" went in, then
  `git status` was not empty, so `RuntimeError: … still dirty after
  committing`.
* #72 (same Dandiset, June 2025) shows the same end state, with raw `fatal:
  Unable to create '…/index.lock': File exists` lines on the cron's stderr.
  They are raw, not logged.  Our `git rm` output is captured and logged, so
  those came from git processes run by git-annex.  Unlike our side, git-annex
  did not retry, and some paths were left unstaged.  (Hypothesis — confirm
  with `git status` on drogon, see §4.4.)

### 4.2 Code

`Downloader.process_blob()` (`asyncer.py:277-298`): a new file ("Not in
dataset; will add") sets `to_update`, and every `to_update` runs
`await self.ds.remove(blob.path)`.  That is a `git rm` (plus DataLad's
`precommit()`) for a path that isn't there.  Each call rewrites the whole
index (O(n²) over a sync), and each opens a window for git-annex's
concurrent `git update-index` to hit our lock.  This is also #62's 20 h
update of 001169.

### 4.3 Fixes

* **F3a (must)**: call `ds.remove()` only in the "hash shows modification"
  branch, where a file is actually being replaced.  A path that isn't in the
  work tree needs no `git rm`: if it is still in the index, `fromkey` /
  `addurl` stage the new entry over it.  This removes all 177,021 needless
  index rewrites and the contention they cause.  The 3 real updates remain.
* **F3b (must)**: put `describe_dirt()` into `AsyncDataset.commit()`'s
  "still dirty after committing" error (`adataset.py:538-542`).  Today the log
  cannot tell what was left.
* **F3c (should)**: don't seal a backup that isn't fully staged.
  `async_assets()` writes and stages `assets-state.json` (= `modified`) and
  commits (`asyncer.py:686-693`) even when files are unstaged or untracked.
  HEAD then claims a complete backup, so the next timestamp run skips 000571
  until its draft changes again — the "Dirty Mirrors" caveat in `CLAUDE.md`.
  Before `set_assets_state()`, check `git status` for unstaged/untracked asset
  paths.  *Decision*: `git add` them with a WARNING and their count (like
  `git_add_manually()` for `need_add`), or fail before sealing so the mirror
  stays visibly stale.  Recommended: `git add` + WARNING, since the files are
  complete annex symlinks or downloaded text, and git-annex only failed to
  stage them.
* **F3d (could)**: for real updates, replace `git rm` with `unlink()` and let
  `fromkey`/`addurl` restage the path, so we never write the index while
  git-annex does.
* Tests: spy on `AsyncDataset.remove` while syncing a Dandiset with only new
  assets (not called) and with one modified asset (called once).  Check that
  `commit()` on a dataset with an untracked file names the file in its error.

### 4.4 Remediation (on drogon)

```
cd /mnt/backup/dandi/dandisets/000571
git status --porcelain | wc -l; git status --porcelain | head -20
```

* If what's left is untracked or unstaged asset files (expected): `git add`
  them and commit, dated like HEAD, then run
  `tools/backups2datalad-update-cron --mode verify 000571`.
* **Don't** `git reset --hard && git clean -dfx` and then rerun.  HEAD's
  `.dandi/assets.json` already lists those assets, so `--mode force` would
  find their metadata unchanged and never re-add the files.  Only `--force
  assets-update` would, which re-checks all ~177k assets.  Tolerable after
  F3a, not before.

## 5. Order of work

1. **PR A**: F1a + F1c + test.  Small, and unblocks §2.5.
2. **PR B**: F3a + F3b + tests.  Small, and removes ~15 h from any
   000571-sized sync.
3. **Ops**: 000571 (§4.4), 001602 force (§3.3), and the Zarr remediation
   after PR A is deployed (§2.5).
4. **Upstream**: dandi-archive report for 001602.
5. **Follow-ups**: F1b, F3c, F3d, and the one-off `--zarr-mode checksum`
   sweep.
6. **Issue hygiene**: point #66 and #90 at F1a (#66's "listing while
   uploading" hypothesis is §2.3), and #72 and #62 at F3a.  Merge #146.

## 6. Not failures, noted in passing

* GitHub secondary rate limits were hit 5× per run and absorbed by
  `GitHubGate` (#122, #145), as designed.
* Transient DANDI API and S3 errors were retried by `arequest()`.
