# Plan: self-healing for Dandiset and Zarr mirrors

Companion to `docs/github-zarr-rate-limits-plan.md`, which owns everything that
needs a GitHub API call to decide.  This document owns local repository state.

**Status.** The Zarr half shipped in #137 as `--zarr-dirty {error,reset+clean}`
(CLAUDE.md, "Dirty Zarrs").  The Dandiset half is designed here and is not
implemented.

**Decisions taken by the maintainer**, which this revision is written to and
which overturn the more cautious stance of this document's first draft:

* Discarding uncommitted state with `git reset --hard` + `git clean -dfx` is an
  acceptable remedy, not a last resort.
* Removing content that has not yet been pushed to GitHub is acceptable.
* Zarr mirrors have local originals under `zarr_root/`, so a Zarr *inside* a
  Dandiset can be dropped and, if ever needed, re-installed cheaply.

Those three together are what make Dandiset healing tractable, because every
hard case in §4.3 reduces to "drop it; the authoritative copy is elsewhere".
The quarantine-instead-of-delete and evidence-ref machinery the first draft
proposed is gone with them.

---

## 1. Root cause: why these mirrors go dirty

The motivating error decodes exactly, and is not random dirt:

```
RuntimeError: Dirty Dandiset 001769/draft; clean or save before running; 2 dirty paths:
M .dandi/assets.json      <- ' M': unstaged
M  dandiset.yaml          <- 'M ': staged
```

* `dandiset.yaml` is **staged** because `update_dandiset_metadata()` rewrites it
  and calls `ds.add()` (`util.py:212-214`).
* `.dandi/assets.json` is **unstaged** because `async_assets()` writes it in a
  `finally:` (`asyncer.py:587-588`) while the `ds.add(".dandi/assets.json")` on
  the very next line sits *outside* the `finally:` and is skipped under
  cancellation.

So any failure inside the asset nursery — a failed Zarr, a download error, a
SIGINT — produces this exact pair.  Two related latent bugs, both still live,
are R1/R2 in §7.

Since late September this no longer *conceals* itself: `update_dandiset()` gates
on `get_backup_state()`, the older of the working-tree and `HEAD` states
(`adataset.py:1073-1084`), so an uncommitted state bump cannot make a mirror
look current.  Before that it could, and 000571 sat dirty and unmentioned from
2026-09-10.  The failure is now loud; what it is not is self-correcting.

---

## 2. Measured facts

Re-verified for this revision.  These are why the Dandiset ladder in §4.2 is
ordered the way it is.

### 2.1 What counts as dirty

`_status_porcelain()` (`adataset.py:270-278`) pins
`--untracked-files=normal --ignore-submodules=none`.  The second is
load-bearing: a Dandiset is dirty if an **installed** Zarr submodule has new
commits, modified tracked content, **or merely one untracked file inside it**.

### 2.2 `reset --hard` + `clean -dfx` cannot clean a Dandiset on its own

Measured on a parent with an installed submodule holding an uncommitted change
and an untracked file:

```
=== status ===                        M sub.zarr
=== reset --hard && clean -dfx ===
=== status ===                        M sub.zarr      <- unchanged
```

Neither command recurses into an active submodule, at any `-f` count or with
`--recurse-submodules`.  **For a Dandiset the remedy is therefore incomplete by
construction**, which is the most important difference from the Zarr case that
shipped.

`git clean` also refuses to remove an untracked directory that is itself a git
repository — the orphaned-Zarr-clone shape from `asyncer.py:607-644`, where the
`clone()` succeeded and `add_submodule()` did not.  Only `-ff` removes that.

### 2.3 Dropping the submodule does clean it, even when the submodule is dirty

Same fixture, continuing:

```
=== datalad drop --what datasets --recursive --reckless kill ===
uninstall(ok): sub.zarr (dataset)
=== parent status ===                 (empty — clean)
=== sub.zarr ===                      (empty directory)
```

`reckless="kill"` drops a submodule with uncommitted *and* untracked content
without complaint, and the parent goes fully clean.  That is exactly
`AsyncDataset.uninstall_subdatasets()` (`adataset.py:1097-1119`), which
`sync_dataset()` already calls at the end of every successful sync.  **The
Dandiset remedy is in large part "run the cleanup step the crashed run never
reached".**

### 2.4 Nothing in the Dandiset flow needs a Zarr submodule installed

Two independent confirmations:

* `update_submodule()` (`adataset.py:1130-1137`) writes the gitlink with
  `git update-index --index-info`.  No checkout is involved, so the normal sync
  path records Zarr commits without ever installing anything.
* `get_stats()` reads a Zarr's contribution via `get_zarr_sub_stats()`
  (`adataset.py:998-1003`), which takes `zarr_id` from `gitmodule_url` and then
  opens **`config.zarr_root / zarr_id`** — the local original, not the submodule
  checkout.  `adataset.py:982` says so outright: "this zarr should not be
  present locally as a submodule".

So the steady state of a Dandiset mirror is *all Zarr submodules uninstalled*,
and healing never has to re-install one.  An installed Zarr submodule at the
entry gate is itself the anomaly.

### 2.5 What a reset actually costs on a Dandiset

* **Bytes: none for annexed content.**  `.git/annex` and the `git-annex` branch
  are untouched by both commands, and blobs are *registered*, not downloaded —
  `process_blob()` takes the `from_key` + `registerurl` path for anything binary
  or over the size limit.  A reset discards index entries and symlinks, not URL
  knowledge; the objects remain, unreferenced.
* **Wall clock: substantial.**  Re-registering N assets is N × (`from_key` +
  2 × `registerurl`) git-annex round trips plus an S3 `HEAD` per blob.  At
  10⁵–10⁶ assets that is hours.  It also invalidates the `dandi.stats` and
  `dandi.populated` caches, both keyed on HEAD.
* **Genuinely lost:** text files ≤ `BACKUPS2DATALAD_TEXT_SIZE_LIMIT` that were
  downloaded into Git and staged but not committed.  Bounded by construction,
  re-downloaded next run.

Cheap in bytes, expensive in time — which is the argument for a budget (§4.6) on
Dandisets even though the shipped Zarr option has none.

### 2.6 The one case where a blind reset is actively dangerous

`mkrelease()` runs `git checkout -b release-<version>` (`datasetter.py:606`,
`:626`) and only returns to `draft` at `:645-646`, **with no `try`/`finally`**.
Anything raising in between — and `sync_dataset()` at `:628` can raise for a
dozen reasons — leaves HEAD parked on the release branch, with
`.dandi/assets-state.json` already rewound to `version.created`, guaranteeing
re-selection next run.

Today that fails loudly at the dirty gate and a human notices the branch.  A
blanket `reset --hard; clean -dfx` makes the mirror look **perfectly clean while
still on the wrong branch**, after which `sync_dataset()` commits the draft state
onto `release-<version>` and `ds.push(to="github", ...)` pushes *that* branch
while `draft` silently stops advancing.

This is Dandiset-specific: Zarr mirrors have no release branches, which is why
the shipped Zarr option needs no branch check and the Dandiset one does.  It is
also worth fixing on its own (R5, §7).

---

## 3. The Zarr case, as shipped

`--zarr-dirty {error,reset+clean}` (#137).  `error` is the default and today's
behaviour; `reset+clean` logs one `ZARR-RESET:` WARNING, calls
`AsyncDataset.reset_hard_clean(check_clean=True)`, and carries on.  Scope falls
out of the call site: `sync_zarr()` is reached through a Dandiset's own asset
listing, so the Dandisets named on the command line narrow it.  `--mode verify`
never discards.  No budget, no journal, no evidence kept — deliberately.

Two properties carry straight over to Dandisets:

* **Re-check, never assume.**  `reset_hard_clean(check_clean=True)` raises if the
  mirror is still dirty afterwards, which is how §2.2's non-convergence surfaces
  as a clear error instead of a confusing downstream failure.
* **One greppable token, no path lists.**  A mirror can have thousands of dirty
  paths; the count is the only part usable at fleet scale.

And one does not: for a Zarr, `reset+clean` is the whole remedy.  For a Dandiset
it is the *last* rung.

---

## 4. Dandiset healing

### 4.1 Where the gate is, and where it must not be

`sync_dataset()` (`datasetter.py:327-331`) — its first statement, before
`AssetTracker.from_dataset()` scans the worktree.  That is the right place and
needs no moving.

Two places that are **not** healing sites:

* `AsyncDataset.commit(check_dirty=True)` (`adataset.py:523`) fires *after* our
  own commit.  Dirt there means our commit did not capture what we staged — a bug
  in this tool operating on freshly produced work.  Resetting it would delete
  exactly what we were trying to save.  Leave it a hard error.
* The **superdataset**.  `update_from_backup()` saves only the gitlinks of
  Dandisets that succeeded (`datasetter.py:137-141`), so it is chronically dirty
  *by design* after any failure, and its dirt is pending submodule registrations
  — real work.  Never heal it; never add a generic sweep over `dandiset_root`.

Note what the gate cannot see: `sync_dataset()` only runs when the committed
state looks stale or the mode is force/verify, so a mirror that is dirty *and*
current upstream is invisible to it.  That is deliberate (CLAUDE.md, "Dirty
Mirrors": `git status` is ~70 ms × ~1400 mirrors) and is what §5.2 is for.

### 4.2 The ladder

Ordered cheapest-and-narrowest first, re-checking `is_dirty()` after each rung
and stopping at the first clean result.  The ordering is forced by §2.2–2.3: the
submodule rung must precede the reset, because the reset provably cannot do that
job.

| # | Rung | Why here |
| - | ---- | -------- |
| 0 | **Preconditions — refuse.**  `MERGE_HEAD` / `rebase-*` / `CHERRY_PICK_HEAD`; unmerged (`U*`) paths; an adjusted branch; a present `.git/index.lock`; `--mode verify`. | None is produced by this tool; a lock may have a live owner. |
| 1 | **Branch.**  If HEAD is not `DEFAULT_BRANCH` (`release-*` or detached), `git checkout draft` **before dirt is evaluated at all**, leaving the stray branch in place as evidence and logging it. | §2.6.  A reset here hides corruption instead of fixing it. |
| 2 | **Drop Zarr submodules.**  `uninstall_subdatasets()`. | §2.3.  Clears submodule dirt, which rungs 3–4 cannot, and is the step the crashed run skipped. |
| 3 | **Discard the rest.**  `reset_hard_clean(check_clean=False)` — the method #137 already added. | Metadata and in-flight asset state. |
| 4 | **Orphaned Zarr clones.**  Remove untracked nested git repositories — §4.3. | `clean -dfx` refuses them (§2.2). |
| 5 | **Re-check.**  Still dirty ⇒ hard error naming what remains. | Non-convergence must be loud, not silent. |

Rung 2 before rung 3 matters twice over: dropping a submodule leaves an empty
directory that `reset --hard` then reconciles cleanly, whereas resetting first
leaves the installed checkout untouched and the mirror still dirty.

### 4.3 Zarr submodules — the strategy

This is the part a Dandiset needs and a Zarr does not.  Every case reduces to
one principle: **`zarr_root/<zarr_id>` is authoritative, the Dandiset's copy is
derived, so the derived copy is always expendable.**

| State in the Dandiset | Porcelain | Action |
| --------------------- | --------- | ------ |
| **Uninstalled** — the steady state (§2.4) | clean | Nothing.  `clean -dfx` correctly leaves the empty directory alone. |
| **Installed and clean** — a crashed run never reached `uninstall_subdatasets()` | clean, or ` M` if its HEAD moved | Drop it (rung 2).  Nothing needs it installed. |
| **Installed and dirty** | ` M <path>` | Drop it (rung 2).  Verified to work with `reckless="kill"` even when dirty (§2.3); uncommitted work in a *checkout* is not the mirror of record. |
| **Untracked nested clone** — `clone()` ran, `add_submodule()` did not | `?? <path>/` | Remove the directory (rung 4). |

**Why the last one is safe.**  Within `dandiset_root/<id>`, an untracked
directory containing `.git` can only have arrived via the Zarr clone at
`asyncer.py:628` (or `backup_zarr()`'s equivalent), both of which clone a Zarr
whose `zarr_root/<zarr_id>` mirror `sync_zarr()` has already created.  Its
content therefore exists locally whether or not it was ever pushed, so
"removing what was not yet pushed is fine" applies with room to spare.

Two ways to implement rung 4:

1. **Targeted** (recommended): for each untracked directory containing `.git`,
   resolve the Zarr id (from `.gitmodules`, or the directory's `github`/`origin`
   remote), confirm `zarr_root/<zarr_id>` exists, and remove the tree.  Roughly
   fifteen lines, and safe *by construction* rather than by assumption.
2. **`git clean -ffdx`**: one flag instead of fifteen lines, and correct under
   the invariant above — but correct by assumption, and it would also remove a
   nested repository that is *not* a Zarr, such as a scratch clone a maintainer
   parked in a mirror while debugging.

I recommend (1), with (2) at most as a config escape hatch, because the value of
rung 5 is being able to say *why* the mirror is clean afterwards.  Note (2) is
also the escalation an operator reaches for unaided if rung 4 is missing and the
heal keeps failing — a reason to implement rung 4 rather than leave it out.

**Re-installing is never required** (§2.4).  If some future caller does need a
checkout, `clone` from `zarr_root/<zarr_id>` plus `add_submodule()` already
exist, and the gitlink the Dandiset records is in that local repo by
construction: `update_submodule()` is only ever called with a `commit_hash`
`sync_zarr()` just committed there.  What to avoid is re-installing from the
submodule's recorded *URL* — with `zarr_gh_org` configured that URL is GitHub
(`asyncer.py:620-624`), which legitimately lags the local original whenever a
push has not happened yet.

### 4.4 Config and CLI

Mirroring the shipped option, so the two read as one feature:

```
--dandiset-dirty {error,reset+clean}     # config: dandiset_dirty
```

`error` by default.  Prefer one shared `DirtyAction` enum over a sibling of
`ZarrDirty`, since the values and semantics are identical.  Reuse #137's
lesson: build the `click.Choice` from the enum's **values**, or `reset+clean` is
unparseable on the command line while the config file requires exactly that
spelling.

Logging follows the shipped shape: one WARNING per healed mirror under a distinct
token — `DANDISET-RESET:` — naming the mirror and which rungs ran, with no path
list, so `grep -c` over a run gives the count that matters.  Naming the rungs is
worth it here because, unlike the Zarr case, what it took to clean a mirror
varies.

### 4.5 Failure semantics, stated honestly

Worth writing into the help text, because #137's first revision got the Zarr
equivalent wrong and claimed it would "fail that Zarr":

`update_dandiset()` runs per Dandiset under `pool_amap` with `config.workers`
concurrency, and a raise fails **that Dandiset**, is recorded in the report, and
makes `update_from_backup()` exit non-zero at the end.  So unlike a Zarr failure
— which cancels up to `ZARR_LIMIT` sibling Zarr syncs inside one Dandiset's task
group — a Dandiset failure does not take other Dandisets down.  That makes
Dandiset healing *safer* to enable than the Zarr one, not riskier.

One caveat to handle rather than inherit: `uninstall_subdatasets()` ends with
`assert all(r["status"] == "ok" for r in res)` (`adataset.py:1116`).  Inside a
heal a single non-`ok` drop would surface as a bare `AssertionError` with no
context.  Rung 2 should tolerate a partial drop and let rung 5 report what is
left.

### 4.6 Budget — the one place not to simply copy the Zarr design

The shipped Zarr option has no counter, and for Zarrs that is right: a reset
costs a re-sync of one Zarr.  A Dandiset reset costs hours of re-registration
(§2.5), and a mirror dirty every single night would silently burn that every
night while looking like a success.

This document originally proposed a `.git/dandi/` incident log with a
three-strike limit.  What survives review of that idea:

* **Clear the counter on a *clean* run, never on a successful one.**  With
  healing enabled every run "succeeds" — because we healed it — so a counter
  cleared by success is unreachable by construction and a chronic fault never
  escalates.  This was the fatal flaw in the original sketch.
* **Key incidents by a signature** — sorted porcelain codes and paths, hashed —
  not by a bare count: the same dirt recurring proves the remedy is treating a
  symptom, while unrelated one-offs months apart should not add up.
* **Append-only, counters derived**, so there is no read-modify-write to lose
  increments; there is no locking anywhere in this tool today (§5.1).
* **`.git/dandi/` is the right home**: already the convention
  (`debug_logfile()`), untracked, never pushed, and correctly lost on re-clone.

Whether to build it now is Q2.  My lean is to ship without, because §5.2 answers
"is this recurring?" more directly and for the whole fleet at once.

---

## 5. Prerequisites and adjacent work

### 5.1 Cross-process locking

Not a nicety: a healer that resets a mirror another process is mid-way through
writing is the worst failure this design can produce, and nothing prevents it
today.

What exists is not a dataset lock.  `AsyncDataset.lock` is an
`anyio.Semaphore(1)` built per *instance* (`adataset.py:65-67`), so two
`AsyncDataset` objects over the same directory hold two unrelated semaphores; it
guards `remove()`/`remove_batch()` within one object and nothing else.
`grep -rn "flock\|fcntl\|LOCK_EX" src test` is empty.  The only lock-aware code
is `_retry_on_git_lock()` (`adataset.py:546-590`), which retries on git's own
`index.lock` and shells out to `fuser -v` — code that exists because this
contention has been *observed*.  `populate` / `populate-zarrs` operate on the
same directories as `update-from-backup`, and a hand-run command is inside no
`flock` at all.

Use `flock(2)` on `<dataset>/.git/dandi/lock`, non-blocking, refusing on
contention.  Not a pid file: pids recycle, so a stale file whose pid has been
reused reads as live and blocks forever, and the check-liveness-then-remove race
cannot be closed in userspace.  `flock` has neither problem — the kernel releases
it when the holder dies, including under SIGKILL — so there is no staleness to
detect and no cleanup to write.  Keep pid/host/command *in* the file as
diagnostics only.  (DataLad already depends on `fasteners`' `InterProcessLock`,
which is lockfile-based and so does have the staleness problem; prefer raw
`flock`.)  Take a parent's lock before any child's, never the reverse, or two
overlapping runs can deadlock — with `LOCK_NB` a cycle degrades to a refusal
rather than a hang.  Confirm the backup roots are local storage; `flock` over NFS
wants a current kernel, and `fcntl.lockf` otherwise.

### 5.2 A dirtiness sweep

There is still no way to ask which mirrors are dirty, and #137 removed the
in-repo helper as redundant.  The only fleet-wide answer remains
`tools/find-INVISIBLE-changed.sh` on drogon — a shell script outside this
repository encoding fleet policy, a liability of its own.

A read-only subcommand needs nothing new: `dandiset_root.iterdir()` /
`zarr_root.iterdir()` for enumeration (as `populate_zarrs` does),
`has_changes(cached=True)` (`adataset.py:298`, ~8 ms) as the cheap tier — the
same `git diff-index --cached --quiet HEAD` the drogon script uses at ~12 s for
all mirrors — and the full `--porcelain` status only on tier-1 hits or under
`--deep`.  Two tiers because one is unaffordable: 001412 alone has 23,614 Zarr
assets, so a full `git status` everywhere is hours.

Tier 1 is sound here precisely because this tool stages everything it writes
(`git annex add`, `git rm`, `git add`), so an interrupted run always leaves work
in the index.

This is the cheapest useful thing left in this document, it is read-only, and it
turns every threshold above into a measurement.

---

## 6. Never heal

* The superdataset (§4.1).
* `commit(check_dirty=True)` (§4.1).
* `UnexpectedChangeError` / `--mode verify`.  Not a repository fault but a
  deliberate assertion that the *server* is consistent; "healing" it converts a
  detected inconsistency into silent data loss.  Its cause has its own fix, the
  quiescent period.
* A stale `.git/index.lock`.  The owner may be alive — this is the bright line.
* `git annex repair` / `fsck` / `dropunused`.  Operator-initiated maintenance.
* Re-pointing a submodule gitlink at a commit the Zarr repo lacks: that rewrites
  what the mirror asserts about history.
* Anything needing a GitHub API call to decide (diverged siblings, orphan repos)
  — owned by the rate-limit plan's `reconcile-zarrs`.

---

## 7. Upstream fixes that reduce the need for this

All still live as of `main` at 603c25a.  R1+R2 together eliminate the dirt in the
motivating error; R5 and R6 are bugs **no healer can reach** and should be fixed
regardless of whether anything here ships.

| # | Fix | Where |
| - | --- | ----- |
| **R1** | Move `tracker.dump()` out of the `finally:`, or shield a restore of `.dandi/assets.json` on the cancellation path | `asyncer.py:587-590` |
| **R2** | `atomic_write_*()` (tmp + `os.replace`) replacing `json_py.dump`, which **unlinks before rewriting** — a kill in that window leaves `assets.json` absent, and `AssetTracker.from_dataset` then silently starts from an empty baseline | `util.py:125-127` + 8 sites |
| **R3** | Make `update_dandiset_metadata()` write → add → commit as one unit | `util.py:212-214` |
| **R4** | A SIGTERM/SIGINT handler converting the signal into a cancellation so shielded cleanup runs at all; today SIGTERM runs *nothing* | `__main__.py` |
| **R5** | Wrap `mkrelease()`'s branch work in `try`/`finally` so HEAD always returns to `draft` (§2.6) | `datasetter.py:606-646` |
| **R6** | `ensure_dandiset_policy()` must commit when `.gitattributes` differs from **HEAD**, not from the desired content: today an interrupted policy commit returns early at `adataset.py:168-169` and the dirt is **permanent and unhealable** | `adataset.py:166-180` |
| **R7** | Make the post-`create` steps of `ensure_installed()` idempotent — a create that died before `initremote` is skipped forever after | `adataset.py:94-100` |
| **R8** | `has_unpushed_commits()` reads `rev-parse --abbrev-ref`, which answers `"HEAD"` when detached, so it warns and returns `False` — a detached Zarr is **silently never pushed**.  Use `symbolic-ref` | `adataset.py:919` |

---

## 8. Implementation order

1. **R5 and R6** — bugs in their own right, and R5 is what makes rung 1 a safety
   net rather than the only thing between a crashed `mkrelease()` and a corrupted
   published mirror.
2. **The sweep (§5.2)** — read-only, shippable alone, and the thing that says how
   often any of this actually fires.
3. **The lock (§5.1)** — a prerequisite for healing a Dandiset, and useful on its
   own against contention `_retry_on_git_lock()` currently only retries.
4. **`--dandiset-dirty`** — rungs 0–5, defaulting to `error`.
5. **A budget**, only if the sweep shows chronic cases (§4.6, Q2).

Tests worth naming, beyond per-rung coverage: that a bare `reset --hard;
clean -dfx` leaves an installed dirty submodule dirty and an orphaned clone
present (encode §2.2, so the rung ordering cannot silently regress); that rung 2
clears a *dirty* submodule (§2.3); that a `release-*` stranding is checked out
before any dirt is touched and the stray branch survives (§2.6); that rung 4
removes an orphaned clone only when `zarr_root/<zarr_id>` exists; that a mirror
still dirty after every rung raises naming what remains; that `--mode verify`
never discards; and that the superdataset is never a healing target.

---

## 9. Open questions

1. **One enum or two?**  `--dandiset-dirty` and `--zarr-dirty` have identical
   values and semantics; I would share a `DirtyAction` enum and keep two config
   fields.  Collapsing to a single `--dirty` governing both is terser but removes
   the ability to enable the cheap healer and not the expensive one.
2. **A budget for Dandisets?** (§4.6)  Ship without, like the Zarr option, or
   build the signature-keyed incident log?
3. **Rung 4 mechanism**: targeted removal with a `zarr_root` check, or
   `clean -ffdx`? (§4.3)
4. **Storage under the backup roots** — local disk or NFS?  Decides `flock` vs
   `fcntl.lockf` (§5.1).  Related: can `populate` overlap `update-from-backup` on
   the same dataset in production, and is the cron `flock` per host, per root, or
   per command?
5. **Should the sweep absorb `tools/find-INVISIBLE-changed.sh`** or sit beside
   it?  Folding it in puts fleet policy under version control and test, but the
   script may carry drogon-specific assumptions.
6. **Does a Dandiset heal want to force a re-sync afterwards?**  After rungs 2–4
   the mirror is clean but its committed state may be behind the server; the
   normal timestamp logic should pick that up on the same run, which I believe is
   sufficient — worth confirming against a real 001412-shaped case.
