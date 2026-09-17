# Plan: self-healing for Dandiset and Zarr mirrors

Status: design, not implemented.  Consolidated from three independent design
passes that were run separately and then reconciled; where they disagreed, §7
records the disagreement and the resolution rather than hiding it.

Companion to `docs/github-zarr-rate-limits-plan.md`, which owns everything that
needs a GitHub API call to decide.  This document owns local repository state
only.

---

## 0. Verdict

The problem is real, recurring, and expensive.  The proposed *mechanism* does
not work, and all three passes reached that conclusion independently, by
measurement rather than by argument.

| Proposal | Verdict |
| -------- | ------- |
| Log a warning and carry on instead of failing the run | **Adopt** |
| `git reset --hard` + `git clean -dfx` as the remedy | **Reject** — measured no-op on the two dominant dirt shapes (§2.2) |
| Bounded retries, then error out | **Adopt**, with two rules instead of one (§4.6) |
| Incident log under `.git/dandi/` | **Adopt** — matches existing precedent (`datasetter.py:735-745`) |
| `cleanups_log.json`, read-modify-write | **Reject** — append-only JSONL, counters derived (§4.5) |
| Delete the file on a successful run | **Reject** — this is the single fatal flaw; it makes the breaker unreachable (§3.1) |

The one-sentence version: **classify the dirt and apply a named remedy to each
class, refuse on anything unrecognised, count incidents by signature in a
journal that a successful run cannot erase, and never run `git clean`.**

---

## 1. Root cause: why these repositories go dirty

The reported failure decodes exactly, and it is not random dirt:

```
RuntimeError: Dirty Dandiset 001769/draft; clean or save before running; 2 dirty paths:
M .dandi/assets.json      <- ' M': unstaged
M  dandiset.yaml          <- 'M ': staged
```

The asymmetry names the fault:

* `dandiset.yaml` is **staged** because `update_dandiset_metadata()` rewrites it
  and calls `ds.add()` (`util.py:211-213`), reached from `datasetter.py:317`.
* `.dandi/assets.json` is **unstaged** because `async_assets()` writes it in a
  `finally:` (`tracker.dump()`, `asyncer.py:532-533`) while the
  `await ds.add(".dandi/assets.json")` on the very next line (`:535`) sits
  *outside* the `finally:` and is skipped under cancellation.

So any failure inside the asset nursery — a failed Zarr, a download error, a
SIGINT — produces this exact pair.  This is fragility **F8** of the rate-limit
plan (`docs/github-zarr-rate-limits-plan.md:232-244`).

The consequence is what makes it expensive: the assets-state timestamp is not
advanced, so the Dandiset is re-selected on every subsequent run
(`datasetter.py:229-235`) and fails at the dirty gate **forever**, until a
human intervenes.  `pool_amap` records the failure (`aioutil.py:437-441`),
`update_from_backup` then raises (`datasetter.py:157-160`), the run exits
non-zero, and `set_superds_description` is skipped — every night.

Two further findings from the root-cause pass, both latent bugs in their own
right:

* **`AssetTracker.dump()` is not atomic.**  It goes through
  `datalad.support.json_py.dump` (`util.py:124-126`), which **unlinks the target
  and then rewrites it**.  A kill inside that window leaves `assets.json`
  either absent — and `AssetTracker.from_dataset` swallows `FileNotFoundError`
  (`util.py:64-65`) and silently starts from an **empty baseline**, so every
  asset looks new — or truncated, which raises an unhandled `JSONDecodeError`
  on the next run.  The empty-baseline path is not a crash; it is a wrong
  answer.  Eight other sites write the worktree non-atomically the same way.
* **There is no SIGTERM handler anywhere.**  Python's default action for
  SIGTERM is immediate termination: no `finally:`, no `atexit`.  An OOM kill, a
  systemd `TimeoutStopSec`, or a host reboot therefore runs *no* cleanup at
  all, and is the path that produces truncated rather than merely stale files.

---

## 2. Measured facts

These were established empirically, and re-verified for this document.  They
are the reason the naive remedy is rejected.

### 2.1 What `git status` considers dirty here

`_status_porcelain()` (`adataset.py:259-267`) pins
`--untracked-files=normal --ignore-submodules=none`.  The second is
load-bearing: a Dandiset is dirty if an **installed** Zarr submodule has
new commits, modified tracked content, **or merely one untracked file in it**.

### 2.2 `git reset --hard && git clean -dfx` is a no-op on the dominant shapes

A parent repository with (a) an installed submodule holding one untracked file
and (b) an untracked nested git repository — the orphaned-Zarr-clone shape from
`asyncer.py:572-589`, where `clone()` succeeded and `add_submodule()` did not:

```
=== status BEFORE ===
 M sub
?? orphan.zarr/
=== running: git reset --hard && git clean -dfx ===
=== status AFTER ===
 M sub                                          <- unchanged
?? orphan.zarr/                                 <- unchanged
=== orphan.zarr annex object still present? ===
-rw-r--r-- 1 root root 8 ... orphan.zarr/.git/annex/objects/KEY
=== what -ff WOULD do ===
Would remove orphan.zarr/
```

Both commands exit 0 and change nothing.  `git clean` refuses to remove an
untracked directory that is itself a git repository, and neither command
recurses into an active submodule at any `-f` count or with
`--recurse-submodules`.

The operational consequence: the heal "succeeds", the gate re-checks
`is_dirty()`, gets `True`, and raises anyway — having consumed an incident.
Three runs later the budget is exhausted.  **Three failed runs and a day of
cron to reach a conclusion the first run could have reached**, plus two
destructive resets that achieved nothing.

The escalation an operator then reaches for — `-ff`, which git's own message
suggests — is the real catastrophe: it removes the orphaned Zarr clone
wholesale, `.git/annex/objects` and all.  That clone may be the only copy of a
Zarr that was never pushed, because a rate-limit give-up is why it is orphaned
in the first place.

### 2.3 `-x` is pure downside

Ignored files never appear in `git status --porcelain`, so they are never part
of the dirt being healed.  `-x` can therefore only ever delete something that
was not the problem — for example a maintainer's `scratch/` added to
`.git/info/exclude` while investigating a Dandiset.

### 2.4 Discarding tracked content is cheap; deleting untracked trees is not

* `.git/annex` is never touched by `git clean`, with or without `-x`.
* A staged `git annex fromkey` symlink discarded by `reset --hard` leaves the
  **object** in `.git/annex/objects`.  The next run computes the same key from
  the same SHA-256 and re-registers it — **the bytes are not re-downloaded.**
  Blobs are registered, not fetched, for anything binary or over the size limit
  (`asyncer.py:293-317`).
* What a reset actually costs is wall clock: N × (`from_key` + 2 ×
  `registerurl`) plus an S3 `HEAD` per blob, and invalidation of the
  `dandi.stats` / `dandi.populated` caches (`adataset.py:999-1009`, `:1095-1098`).
  Hours at 10⁵–10⁶ assets, but not bytes.
* Only text files ≤ `BACKUPS2DATALAD_TEXT_SIZE_LIMIT` go into Git via
  `addurl --with-files` (`:319-325`); those are re-downloaded.  Bounded by
  construction.

### 2.5 A live bug found by all three passes: stranding on `release-*`

`mkrelease()` runs `git checkout -b release-<version>` (`datasetter.py:531`
and `:551`) and only returns to `draft` at `:569-570`.  **There is no
`try`/`finally`.**  Anything raising in between — and `sync_dataset()` at
`:552-557` can raise for a dozen reasons — leaves HEAD parked on the release
branch.  `mkrelease` has also already rewritten `.dandi/assets-state.json` to
`version.created` (`:534`), an older timestamp, guaranteeing the Dandiset is
re-selected next run.

Today that fails loudly at the dirty gate and a human notices the branch.
**A blanket `reset --hard; clean -dfx` makes it look perfectly clean while
still on the wrong branch**, after which `sync_dataset()` commits the full
draft state onto `release-<version>` and `ds.push(to="github", ...)`
(`datasetter.py:261-266`) pushes *that* branch, while `draft` silently stops
advancing.  The tag then sits several commits behind a branch claiming to be
that release.

This is arguably more damaging than the dirt, nothing in the codebase checks
the current branch, and it should be fixed as a standalone change ahead of
this feature (§6, R5).

---

## 3. Ranked shortcomings of the proposal as stated

**3.1 — "Delete the file on a successful run" makes the breaker unreachable.
(Fatal.)**
The heal happens at the top of `sync_dataset()` (`datasetter.py:293`); the sync
then completes and commits, which *is* "a successful run/commit", which deletes
the log.  Scenario: 001769 is dirty every run because one Zarr fails every run.
Run 1: heal, sync, commit, delete log.  Run 2: identical.  Run 500: identical.
The counter never exceeds 1, the underlying bug lives forever, and the tool has
quietly converted a loud daily failure into a silent daily data-discard.
*Fix: clear on a **clean** run — nothing to heal — never on a successful one,
and never delete the journal.*

**3.2 — The remedy does not converge (§2.2). (Critical.)**
Measured no-op on both dirty-submodule and orphaned-clone dirt, which per the
rate-limit plan's failure matrix are the dominant shapes.  The budget is spent
on non-progress.

**3.3 — The natural escalation to `-ff` is unbounded data loss. (Critical.)**
See §2.2.  The design must make it unreachable, which it does by never invoking
`git clean` at all.

**3.4 — A blind reset cements the `release-*` stranding (§2.5). (Critical.)**
It converts a loud, visible failure into silent corruption of a published
mirror.

**3.5 — "3 subsequent incidents" is the wrong shape of threshold. (High.)**
It conflates two pathologies.  *Same dirt every run* proves the remedy does not
work — three strikes is two destructive actions too many.  *Different dirt
occasionally, forever* is a dataset that is healing correctly and should never
become fatal, only visible.  A bare count also lets cron cadence silently set
the alarm latency: three strikes is three hours hourly and three weeks weekly.

**3.6 — Non-convergent dirt loops forever without a signature rule. (High.)**
Move a backup root to a filesystem that does not preserve the executable bit
and every file reports as modified; `reset --hard` rewrites them and the mode
comes back wrong.  Without a same-signature rule this burns the budget every
day and produces a thousand identical warnings.

**3.7 — A read-modify-write JSON counter loses increments. (Medium-high.)**
`grep -rn "flock\|fcntl\|LOCK_EX" src test` returns nothing; the only
lock-shaped thing is `_retry_on_git_lock()` (`adataset.py:535-590`), which
exists because index-lock contention has been *observed* in production.
`populate`/`populate-zarrs` (`__main__.py:399-527`) touch the same directories.
Lost increments fail in the direction that *weakens* the breaker.

**3.8 — A per-dataset counter cannot see a global cause. (Medium-high.)**
The backup host reboots mid-run; 700 Dandisets are dirty; each is healed once,
each run succeeds, every counter resets.  The event is invisible except as 700
warnings nobody greps for.

**3.9 — Healing silently weakens `Mode.VERIFY`. (Medium.)**
`error_on_change=True` (`datasetter.py:242-245`) is a deliberate, attended
diagnostic.  A maintainer running `--mode verify` to find out what changed
locally must not have it discarded first.

**3.10 — The evidence is destroyed. (Medium.)**
`describe_dirt()` (`adataset.py:272-285`) exists because a previous review
decided the log alone should tell an operator what needs cleaning.  Discarding
the state unrecorded reverses that decision, and `describe_dirt()`'s 10-line
truncation is inadequate for a heal that throws state away.

**3.11 — Healing the superdataset would be actively wrong. (Medium.)**
`update_from_backup()` saves only the gitlinks of Dandisets that succeeded
(`datasetter.py:137-141`), so the superdataset is chronically dirty *by design*
after any failure.  A generic "heal any dirty dataset" would revert real
submodule pointer advances.

**3.12 — It treats a symptom.**  Say so out loud (§6): the reported dirt is
manufactured by a two-line ordering bug.

---

## 4. The consolidated design

### 4.1 Principles

1. **Classify before acting.**  No blanket verb.
2. **Allowlist, not denylist.**  Anything unrecognised refuses the whole heal.
   This is also the answer to "self-healing hides bugs": a regression producing
   *new* dirt classifies as `UNKNOWN` and fails on day one, whereas
   `reset --hard` heals precisely the thing you want to be told about.
3. **Move, never delete.**  `git clean` does not appear in the implementation.
4. **Prefer completing the interrupted step** over undoing it.
5. **Never move HEAD or any ref.**  Remedies restore a child to *its own* HEAD,
   so the parent's recorded gitlink stays correct by construction.
6. **Evidence is nearly free; keep it.**
7. **Measure before enabling destruction.**

### 4.2 Classification

Input: `git status --porcelain=v2 -z --untracked-files=normal
--ignore-submodules=none` (v2 exposes submodule sub-state directly).

Preconditions checked **before** any dirt is examined — each refuses or is its
own named remedy:

| Precondition | Action |
| ------------ | ------ |
| HEAD not on `DEFAULT_BRANCH` (`consts.py`, `"draft"`), incl. detached and `release-*` | `CHECKOUT` draft; leave the stray branch in place as evidence; re-evaluate (§2.5) |
| Adjusted branch (`refs/heads/adjusted/…`) | **Refuse** |
| `.git/index.lock` present | **Refuse** — the owner may be alive |
| `MERGE_HEAD` / `rebase-*` / `CHERRY_PICK_HEAD` | **Refuse** |
| `error_on_change` / `Mode.VERIFY` | **Refuse** (§3.9) |

Then, per porcelain entry:

| Class | Match | Remedy |
| ----- | ----- | ------ |
| `META` | path ∈ {`dandiset.yaml`, `.dandi/*`, `.datalad/config`, `.datalad/providers/*`, `.gitattributes`, `.gitmodules`}, any status | `git restore --source=HEAD --staged --worktree -- <paths>` |
| `SUBMODULE` | v2 sub-state says a submodule is modified / has untracked content / differs from the gitlink | `uninstall_subdatasets()` (`adataset.py:1053-1075`) — the cleanup step the crashed run never reached, and what the happy path already does at `datasetter.py:334-336` |
| `NESTED_CLONE` | untracked directory containing `.git`, at a Zarr asset path | If `zarr_root/<id>` exists and the clone's HEAD is an ancestor of it → **complete** the interrupted `add_submodule()`.  Otherwise **refuse and report** — it may hold unpushed commits |
| `STAGED_ASSET` | staged annex symlink under an asset path | `git rm --cached` only; the annex object is never touched.  *Non-default* |
| `UNMERGED` | `u` records | **Refuse** |
| `UNKNOWN` | anything else | **Refuse the whole heal** |

For the reported 001769 failure the entire remedy is one command:
`git restore --source=HEAD --staged --worktree -- .dandi/assets.json dandiset.yaml`.

**Always re-verify.**  `is_dirty()` is re-run after the plan; residual dirt is
an immediate hard failure (`outcome: residual-dirt`), not a consumed strike.
That single rule is what would have caught §2.2 in the field rather than in a
document.

### 4.3 Hook points

Three, and one deliberate non-hook:

| Where | Change |
| ----- | ------ |
| `datasetter.py:293-297` | `ensure_healthy(..., kind="dandiset")` |
| `zarr.py:571-575` | `ensure_healthy(..., kind="zarr")` |
| end of `sync_dataset()` / `sync_zarr()` | record a `clean` event iff the dataset is clean and on `DEFAULT_BRANCH` |
| **`adataset.py:478-482`** | **unchanged — never heal here** |

The last is the assertion "our own commit did not capture what we staged".
That is a bug in *this* tool operating on freshly produced work; resetting it
would delete exactly what we were trying to save.

No generic sweep over `dandiset_root` — see §3.11.  The heal must also run
before `AssetTracker.from_dataset()` (`datasetter.py:303`), which the current
guard position already satisfies.

### 4.4 Persistence

`<dataset>/.git/dandi/heal/journal.jsonl` — **append-only**, one JSON object
per line, one `write()` per record, `O_APPEND`, each record capped so appends
cannot interleave.  Precedent: `debug_logfile()` already owns `.git/dandi/`
(`datasetter.py:735-745`), and `.git/config` already carries per-dataset
bookkeeping under `dandi.*` (`adataset.py:999-1014`, `manager.py:46-57`).

`.git` is right, and the "lost on a fresh clone" property is correct
semantics, not a defect — a fresh clone genuinely has no incident history on
this host.  It must **not** go in the mirror's tracked content or the
`git-annex` branch: these mirrors are a public product and host-local
operational noise must not propagate to GitHub and to every clone.

Record schema (`version: 1`, checked on read; unknown versions are reported and
ignored):

```jsonc
{
  "version": 1,
  "time": "2026-09-17T04:12:03.117Z",
  "run": "2026.09.17.04.00.00Z",        // ties to the debug log
  "host": "drogon", "pid": 21414,
  "kind": "dandiset", "desc": "Dandiset 001769/draft",
  "head": "9f1c…", "branch": "draft",
  "signature": "6a1f…",                 // sha256 of sorted "XY\tpath", content-independent
  "classes": {"META": 2},
  "entries": [{"xy": " M", "path": ".dandi/assets.json", "class": "META"}],
  "remedies": [{"kind": "RESTORE", "paths": [".dandi/assets.json", "dandiset.yaml"]}],
  "evidence": {"ref": "refs/dandi/heal/2026-09-17T04:12:03Z", "quarantine": null},
  "porcelain": "…full output, truncated with an explicit marker…",
  "outcome": "healed"                   // healed | reported | residual-dirt | refused | failed
}
```

Counters are **derived** from the journal, never stored — that is the whole
reason for append-only, and it removes read-modify-write from the design
entirely (§3.7).  A per-dataset non-blocking `flock` on
`.git/dandi/heal/lock` is held across classify → evidence → act → record;
if it is held, refuse rather than race.

### 4.5 Evidence

Before any mutation:

1. **Always** — the full untruncated porcelain, HEAD, branch, and diffstat into
   the record.
2. **Tracked dirt** — `git stash create` (which writes a commit object and
   moves nothing) then `git update-ref refs/dandi/heal/<ts> <sha>`.  For
   annexed paths this is a symlink diff: kilobytes, not gigabytes.  The ref is
   what protects it from the `ds.gc()` that runs later in the same flow
   (`datasetter.py:337-339`).  Recovery is `git stash apply refs/dandi/heal/<ts>`.
   Deliberately **not** `git add -A` + `write-tree`, which in an annex repo
   would commit untracked binaries straight into Git.
3. **Untracked paths** — `os.replace`d into
   `.git/dandi/heal/quarantine/<ts>/`, same filesystem, O(1) even for a large
   tree, with a manifest recording original paths, sizes and whether the entry
   was a nested repo.

Retention is swept only by an explicit command, never as a side effect of a
backup run.

**Why discard rather than commit the dirt.**  Committing looks conservative,
and it is decisively wrong here: `mkrelease()` selects a release-tag target by
scanning commits matching `--grep=\[backups2datalad\]` and comparing
`.dandi/assets.json` against the published asset list
(`datasetter.py:463-492`, `:503-510`).  A heal-commit whose `assets.json` was
dumped after `finish_asset()` but before the files were added can **match the
remote asset list while its tree is missing those files** — and the resulting
tag is pushed and a GitHub release created (`:573`, `:576-578`).  Tags are
permanent and public.  Commit dates are also load-bearing throughout this tool
and a heal has no natural date.  If a heal ever does commit, its message must
not contain `[backups2datalad]`.

### 4.6 Circuit breakers

**Per dataset, two rules, because one is not enough:**

* **Consecutive same-signature**, default 3 — the proposal's rule, kept, but
  keyed on the signature.  The same fault recurring means the remedy is
  treating a symptom whose cause is still there.  Keying on signature is what
  makes 3 a sane number: unrelated one-off interruptions in March and September
  do not add up.
* **Rate**, default 5 in 30 days, any signature — catches the flapper the
  consecutive rule misses (dirty / clean / dirty / clean … never reaches
  `consecutive > 1` yet is obviously broken), and the chronic dataset the
  proposal's delete-on-success hides entirely.

Exceeding either raises `HealBudgetExceeded`.  A dataset that goes dirty once a
month for unrelated reasons is never fatal — it surfaces in the report as
chronic, which is the correct outcome.

**Fleet-wide**, modelled on `GitHubGate` (`aioutil.py:161-396`): one `Healer`
per process held on `Manager` (shared by every worker, because
`with_sublogger()` is `dataclasses.replace`, `manager.py:36-37`).  Trip when
more than 25 datasets healed, or more than 5 % of at least 50 visited.  On
trip, set `gave_up`, log `GAVE-UP:`, and degrade every subsequent detection to
today's raise.  Do **not** abort the run — aborting mid-run leaves more
half-finished datasets, which is the thing being fixed.

**Cadence caveat, and a detection bias worth knowing:** thresholds are in
incidents, so alarm latency scales with cron cadence.  And `sync_dataset()` —
hence `is_dirty()` — only runs when the server timestamp advanced or the mode
is force/verify (`datasetter.py:229-250`), so **a Dandiset that is dirty *and*
unchanged upstream is invisible to the detector entirely**.  The reporting
command must therefore walk the mirror tree directly, as `populate` does
(`__main__.py:439-451`), not piggyback on the sync path.

### 4.7 Config, CLI, logging

Following the `Mode`/`quiescent_period` house style — `StrEnum` in `config.py`,
`click.Choice` in `__main__.py`, thresholds in `consts.py`, `default_factory`
so the suite can patch the constant:

```
--heal {off,report,on}     default: report
--heal-dry-run             classify and log the plan, change nothing
backups2datalad heal-report [--json] [--tripped-only]   # read-only, walks the tree
backups2datalad heal-report --clear DATASET             # un-trip, leaves a trace
backups2datalad heal-report --prune [--older-than 30d]  # sweep refs + quarantine
```

`report` classifies, records an incident, logs, and then raises exactly as
today — so two weeks of production produce real numbers rather than zeroes,
and flipping to `on` is a one-line change backed by data.

Logging uses stable greppable tokens, mirroring the existing
`RATELIMIT:`/`GAVE-UP:` discipline: `HEAL:` at **WARNING** with the full
porcelain and the evidence ref, `HEAL REFUSED:` at ERROR with the rule that
tripped and the command to clear it, and one `HEAL SUMMARY:` line per run.
Discarding a staged, already-downloaded text asset is bounded but real data
loss; it does not belong at INFO.

The test suite gets an autouse `no_healing` fixture mirroring
`no_quiescent_period` (`test/conftest.py:83-90`); healing tests opt in
explicitly, as `test_quiescence.py` does.  Several existing tests assert the
`Dirty …` error path and must keep doing so.

---

## 5. Scope

**In:** dirty worktree at the two gates; HEAD parked on `release-*` or
detached; installed subdatasets left behind; orphaned Zarr clone (complete, or
refuse).

**Also in, and where most of the fleet signal will come from:** the tool
**already self-heals silently in six places** — `adataset.py:142-169` (policy
migration, which even makes commits), `:592-599` (`gc` rc 128), `:727-772`
(embargo URL fixups), `:846-851` (restoring sibling config), `syncer.py:190-359`
(Zarr submodule URL/privacy fixups), `asyncer.py:451-455` and `:537-543`
(`addurl` "exited 123" → manual `git add`).  None is counted, none has a
budget, three log at INFO or below.  If the policy migration started firing on
every run for every Dandiset, nothing would say so.  Folding these into the
framework is cheap: they keep their code and gain a journal record.

**Out, firmly:**

* `git clean` in any form, and `git annex repair` / `fsck` / `dropunused`.
* Anything needing a GitHub API call to decide — diverged siblings, orphan
  repos.  Owned by the rate-limit plan's `reconcile-zarrs`.
* `UnexpectedChangeError` (#119).  It is not a repository fault but a
  deliberate assertion that the *server* is consistent; "healing" it converts a
  detected inconsistency into silent data loss.  Its cause already has a fix —
  the quiescent period.
* Stale `.git/index.lock` removal.  The owner may be alive; this is the bright
  line.
* The superdataset (§3.11) — `report`-only.
* Re-pointing a submodule gitlink at a commit the Zarr repo lacks: that rewrites
  what the mirror asserts about history.

---

## 6. Upstream fixes that reduce the need for this

The healer treats a symptom.  These are small, mechanical, testable without
docker, and **R1+R2 together eliminate the dirt in the reported error**:

| # | Fix | Where |
| - | --- | ----- |
| **R1** | Move `tracker.dump()` out of the `finally:`, or shield a restore of `.dandi/assets.json` on the cancellation path | `asyncer.py:532-535` |
| **R2** | `atomic_write_*()` helper (tmp in same dir + `os.replace`); replace `json_py.dump` in `AssetTracker.dump()` | `util.py:124-126` + 8 sites |
| **R3** | Make `update_dandiset_metadata()` self-contained: write → add → commit as one unit | `util.py:191-213` |
| **R4** | SIGTERM/SIGINT handler converting the signal into a cancellation so shielded cleanup runs at all | `__main__.py` |
| **R5** | Wrap `mkrelease()`'s branch work in `try`/`finally` so HEAD always returns to `draft` (§2.5) | `datasetter.py:525-570` |
| **R6** | `ensure_dandiset_policy()` commits when `.gitattributes` differs from **HEAD**, not from the desired content — today an interrupted policy commit returns early at `adataset.py:158` and the dirt is **permanent and unhealable** | `adataset.py:142-169` |
| **R7** | Make the post-`create` steps of `ensure_installed()` idempotent | `adataset.py:113-138` |
| **R8** | `has_unpushed_commits()` uses `rev-parse --abbrev-ref`, which returns `"HEAD"` when detached, so it warns and returns `False` — a detached Zarr is **never pushed**.  Use `symbolic-ref` | `adataset.py:902-918` |

Ship R1, R2, R5 and R6 before or alongside the healer, and instrument the
healer so the residual dirt rate can be watched to fall.  Ship only the healer
and you will never learn whether the root causes mattered, while every future
crash silently consumes a heal budget.

---

## 7. Where the three passes disagreed, and the resolution

| Question | Positions | Resolved |
| -------- | --------- | -------- |
| Is `git clean -dfx` dangerous in an annex repo? | A: yes, catastrophic.  B: no — `.git/annex` is untouched and `-x` is out of the blast radius; it is *insufficient*, not unsafe.  C: wrong remedy regardless | **B is right on the mechanism, A on the operational risk.**  Plain `-dfx` destroys nothing in `.git/annex` — but it also fixes nothing, and the escalation it invites (`-ff`) is unbounded loss.  Drop `-x` as pure downside; drop `git clean` entirely because nothing needs it |
| Untracked nested repo | A: quarantine by rename.  B: refuse outright.  C: report-only | **Refuse-and-report by default; complete the interrupted `add_submodule()` when `zarr_root/<id>` confirms it is safe; quarantine only under an explicit flag.**  A's insight — finish the interrupted step rather than undo it — is better than all three defaults |
| What clears the counter | A: a completed sync that ends clean.  B: a rolling window.  C: only a run that found nothing to heal | **C.**  A's rule still clears after a successful heal, which is the fatal flaw in a thinner disguise; A's rate window catches it only by accident |
| Counter storage | A: `.git/config` + JSONL ledger.  B: `heal.json` + `os.replace`.  C: derive from an append-only journal | **C** — no read-modify-write anywhere.  A `.git/config` sticky trip flag may be added later |
| Strike threshold | A: refuse on the 2nd identical signature.  B and C: 3 | **3 consecutive same-signature, plus a rate rule.**  A's argument for 2 is sound (one destructive action instead of two) and is a cheap config change if the field data supports it |
| Fleet breaker | A: none.  B and C: yes | **Adopt** — a reboot dirtying 700 Dandisets must page someone |
| Default mode | A: `off`.  B: `heal`.  C: `report` | **`report`** — it is the dry run, it produces real numbers, and flipping to `on` then becomes a one-line change backed by data |

---

## 8. Implementation phases

0. **Upstream fixes** R1, R2, R5, R6 — independently justified, no new concepts.
1. **Detector, read-only.**  `status_records()`, `current_branch()`, the
   classifier, the journal, and `heal-report`.  Wire into the two gates in
   `report` mode: classify, record, then raise exactly as today.  Zero
   behaviour change.  Run it in production for a fortnight and publish the
   distribution — every threshold above is a guess until this exists.
2. **Remedies.**  `META`, `SUBMODULE`, `CHECKOUT`, evidence, breakers,
   `heal-report --clear` / `--prune`.  Default still `report`.
3. **The rest.**  `NESTED_CLONE` completion, `STAGED_ASSET`, folding in the six
   existing silent self-heals.
4. **Flip the default to `on`** once phase-1 data supports it, and document it
   in `CLAUDE.md` beside the Quiescent Period and Force-Push sections.

Tests worth calling out, beyond per-class unit coverage of the classifier
(which is a pure function over porcelain records, so that is where density
belongs): regression tests that **encode the measurements** — that a bare
`reset --hard; clean -dfx` leaves a dirty submodule dirty and an orphaned clone
present, that the orphaned clone is never deleted, that a `release-*` stranding
is checked out before dirt is touched, that an identical signature refuses
without acting, that `--heal-dry-run` leaves the tree bit-identical, that
`Mode.VERIFY` never heals, and that a successful heal does **not** clear the
counter while a clean run does.  Integration: monkeypatch
`update_dandiset_metadata` to raise after its `ds.add()`, reproducing the
reported state exactly, then assert run 1 fails, run 2 heals and completes, and
run 3 is a clean no-op.

---

## 9. Open questions

1. **Thresholds vs. cadence.**  What is the real cron cadence?  It decides
   whether "3 consecutive" means three hours or three weeks.
2. **Default mode.**  `report` first, as proposed, or `off` until asked for?
3. **Strike count.**  3 consecutive same-signature, or 2 (§7)?
4. **`STAGED_ASSET` scope.**  Heal only the `.dandi/` + `dandiset.yaml`
   metadata set, or also half-finished asset registrations?  The narrow version
   heals the reported case and nothing else.
5. **`NESTED_CLONE` forward-completion** is the only remedy that *adds* index
   state.  Enable it, or keep it report-only indefinitely?
6. **Quarantine disk.**  Worst case is one orphaned Zarr clone sitting in
   `.git/dandi/heal/quarantine/` for the retention period.  Acceptable, or
   should large trees refuse instead?
7. **Sticky trips.**  Should a tripped dataset stay tripped until an operator
   clears it, or should the next clean run un-trip it?
8. **Concurrency.**  Can `populate` / `populate-zarrs` overlap
   `update-from-backup` on the same dataset in production?  It decides whether
   the per-dataset `flock` is load-bearing or belt-and-braces.
9. **R5** (`mkrelease` stranding) — confirm it is real and not prevented by
   something outside the repository; if real it should land ahead of this
   feature.
