# Plan: surviving GitHub secondary rate limits when creating Zarr repositories

Status: **draft for review** (2026-09-14).  Comment inline on the PR, or edit
this file directly.  Nothing below is implemented yet.

## 1. The incident

`update-from-backup` failed with

```
create_sibling_github(error): [unauthorized: You have exceeded a secondary rate
limit and have been temporarily blocked from content creation. ...]
create_sibling_github(ok): [sibling repository 'github' created at
https://github.com/dandizarrs/0596fd61-17af-484f-aa1d-a6052949a950]
```

while creating GitHub repositories for a batch of new Zarrs, and left
repositories under https://github.com/orgs/dandizarrs behind that (a) have the
description `some default`, and/or (b) have no content pushed.

The two lines above come from *different* Zarrs: `create_sibling_github` runs
in worker threads (`anyio.to_thread.run_sync`) and DataLad's result renderer
prints each record to stdout as it happens, so records from concurrent calls
interleave.  The `(error)` record is the interesting one.

## 2. What the code actually does today

### 2.1 Every call that talks to GitHub

| Operation | Where | Retry / throttle today |
|---|---|---|
| `POST /orgs/{org}/repos` (repo creation) | `AsyncDataset.create_github_sibling()` (`adataset.py:751`) → DataLad `create_sibling_github` in a thread | **none** — DataLad uses plain `requests` |
| `PATCH /repos/…` (description, homepage, visibility) | `GitHub.edit_repo()` (`manager.py:146`) | `arequest(retry_on=[403, 404])` |
| `GET /repos/…` (visibility check) | `GitHub.get_repo()` (`manager.py:132`) | `arequest(retry_on=[403])` |
| `POST /repos/…/releases` | `GitHub.create_release()` (`manager.py:156`) | **none** (`retry_on` empty) |
| `git push github` | `AsyncDataset.push()` (`adataset.py:477`) | retries only on `"unexpected disconnect"` |
| `git push github <tag>` | `datasetter.py:550` | none |

Call sites of repo creation: `DandiDatasetter.ensure_github_remote()`
(`datasetter.py:180`, Dandisets) and `sync_zarr()` (`zarr.py:559`, Zarrs).

Concurrency that feeds the burst: up to `ZARR_LIMIT = 10` concurrent
`sync_zarr()` (`consts.py:16`, `config.zarr_limit`), on top of
`DEFAULT_WORKERS = 5` Dandisets in parallel.  A batch of new Zarrs means up to
10 repo creations in flight at once, repeated as fast as syncs complete.

### 2.2 What DataLad does on a 403 (checked against `datalad/datalad@38368d7`)

`datalad/distributed/create_sibling_ghlike.py`:

* `repo_create_request()` POSTs with plain `requests` (`:494-499`); no retry.
* `repo_create_response()` maps 401/403 to a result record
  `status='error', message=('unauthorized: %s', <API message>)` (`:549-554`).
  The `Retry-After` header and the status code are **not** preserved.
* `create_repos()` yields that record and `continue`s, i.e. it does not
  configure the local sibling (`:427-429`).
* `@eval_results` then raises `IncompleteResultsError`; the failed record
  (with its message) is available as `e.failed`.
* 422 "already exists" + `existing='reconfigure'` → `repo_get_request()` →
  `status='notneeded'` and the sibling *is* configured (`:293-308`).  So
  re-running after a crash between "repo created" and "sibling configured" is
  already safe.
* Description: `desc_text = description if description is not None else
  'some default'` (`:478`).  `create_github_sibling()` never passes
  `description=`, so **every repo we create is born with the description
  `some default`** and only gets a real one later from
  `set_zarr_description()` / `set_dandiset_description()`.

Consequences: a 403 means the repository was *not* created (retrying is safe
and idempotent); the failure surfaces as `IncompleteResultsError` with the
GitHub message text but without `Retry-After`.

### 2.3 Our own retry helper

`arequest()` (`aioutil.py:119`) retries on `httpx.RequestError`, 5xx, and any
status in `retry_on`, sleeping `exp_wait(attempts=15, base=2)`:
1, 2, 4, … 16384 s — up to ~4.5 h for a single sleep and ~9.1 h in total.  It
does not read `Retry-After` / `x-ratelimit-reset`, does not retry 429, and
cannot tell a secondary-rate-limit 403 from a bad-token 403.

## 3. Fragilities this exposes

* **F1 — repo creation is the one unprotected content-creating call**, and the
  most bursty one.  Everything in `manager.py` has some retry; the DataLad call
  has none.
* **F2 — the block is per token, not per request.**  GitHub's message says
  "temporarily blocked from content creation": once one worker trips it, all
  10 are blocked, and per GitHub's guidance continuing to send requests while
  blocked can extend the block.  Independent per-call retries (even with
  backoff) do not model that; a *shared* cooldown does.
* **F3 — one failed Zarr cancels its siblings.**  `sync_zarr()` is
  `start_soon`'d into the Dandiset's asset nursery (`asyncer.py:358`, task
  group at `asyncer.py:516`); the exception cancels every in-flight Zarr for
  that Dandiset mid-way — repo created, nothing pushed.  This is where the
  half-brewed repositories come from.
* **F4 — "needs push" and "needs description" are not durable state.**  Both
  are gated on *did this run make a commit*: `zarr.py:588` (`made_commit`),
  `zarr.py:607` (push), `zarr.py:625` (description); same pattern for
  Dandisets at `datasetter.py:240-259`.  After a cancelled run the local
  commits exist, so the next visit computes `made_commit = False` and **never
  pushes and never describes**, short of `--zarr-mode force`.
* **F5 — `set_zarr_description()` addresses the wrong dataset under
  `backup-zarrs`.**  `manager.py:113` builds
  `AsyncDataset(config.zarr_root / zarr_id)`, but `backup_zarrs()` syncs in
  `partial_dir / zarr_id` and moves it afterwards (`datasetter.py:579,596`).
  For a non-existent path DataLad's `Dataset.config` silently returns the
  *global* `datalad.cfg` (`datalad/distribution/dataset.py:310-335`), so the
  `dandi.github-description` cache read/write happens outside the Zarr (and is
  shared across Zarrs).  The regular `update-from-backup` path passes the right
  directory (`asyncer.py:353`) and is not affected.
* **F6 — `create_release()` and the tag push have no rate-limit handling.**
  Releases are content-creating too; a fresh mirror of a Dandiset with many
  published versions is another burst (`datasetter.py:550-555`).
* **F7 — an interrupted Zarr sync leaves a dirty dataset that blocks every
  later run.**  `sync_zarr()` raises `"Zarr … is dirty; clean or save before
  running"` (`zarr.py:566`) before `ZarrSyncer` gets a chance to converge.
  Not caused by rate limits, but F3 makes it far more likely to happen.  Noted
  here; whether to change it is an open question (§9).

## 4. Constraints for the fix (maintainer feedback)

1. Keep using DataLad's `create_sibling_github`; do not reimplement repo
   creation.
2. The system has been very robust; do not disturb unrelated code paths.  In
   particular the Dandiset-side flow stays as is unless a change is clearly
   needed there too.
3. Prefer *reactive* throttling — react to the specific failures as they
   emerge, plus occasional sampling of GitHub's rate-limit usage report — over
   a new proactive choke point for all traffic.  Reuse an existing gate if one
   fits.

## 5. Existing gates, and what they do / do not cover

| Gate | Covers | Notes |
|---|---|---|
| `config.zarr_limit` (`CapacityLimiter(10)`) | whole `sync_zarr()` body | the only thing bounding concurrent repo creation; also bounds downloads, so not a rate-limit knob |
| `config.workers` (5) | Dandisets in parallel | same |
| `Manager.gh` — one `GitHub` (one `httpx.AsyncClient`) per process | all `manager.py` API calls | shared by every worker: a natural home for per-process "GitHub is cooling down" state without any new module |
| `arequest()` | all `manager.py` API calls | the one place to teach `Retry-After` / 429 to, benefiting every caller without touching call sites |
| `AsyncDataset.push()` retry loop | pushes | only "unexpected disconnect" |

Nothing covers the DataLad call, and nothing is shared between workers once a
block is in effect.  There is no existing choke point to reuse for content
creation; the proposal below adds the narrowest possible one (a cooldown that
is inert until GitHub actually complains) rather than a global limiter.

## 6. Proposed changes

Ordered by value/risk.  Each item lists its blast radius.  C1–C4 are the
recommended core; C5–C7 are small; C8 is deferred.

### C1 — teach `arequest()` about rate-limit responses (`aioutil.py` only)

* Recognise a rate-limit response independently of `retry_on`: status 403 or
  429 whose body mentions `secondary rate limit` / `abuse detection`, or whose
  `x-ratelimit-remaining` is `0`.
* Sleep the *right* amount: `Retry-After` if present; else until
  `x-ratelimit-reset` when `x-ratelimit-remaining == 0`; else ≥ 60 s with
  exponential growth.  Cap a single sleep (e.g. 15 min) and the total retry
  budget (e.g. 2 h) instead of today's 9 h; add jitter.
* Keep existing `retry_on` semantics for everything else (so `get_repo` /
  `edit_repo` behave exactly as now for non-rate-limit 403s).
* Log the classification and the rate-limit headers at the retry site (the
  headers are already collected by `_describe_http_error()`).

Blast radius: only the retry loop; no call site changes.

### C2 — one shared, reactive cooldown on `GitHub` (`manager.py`)

Add to the `GitHub` class:

```python
cooldown_until: float = 0.0            # monotonic time
async def wait_if_cooling_down(self) -> None: ...
def note_rate_limited(self, retry_after: float | None) -> None: ...
```

* `note_rate_limited()` is called from C1 (when `arequest` sees a rate-limit
  response) and from C3 (when DataLad reports one).  It extends
  `cooldown_until` to now + (`Retry-After` or 60 s, growing on repeated hits).
* `wait_if_cooling_down()` is awaited *before* every content-creating /
  mutating GitHub operation: repo creation (C3), `edit_repo`, `create_release`.
* When nothing has tripped, both are no-ops — the current behaviour is
  unchanged until GitHub actually complains, then all workers back off
  together instead of each burning its own retries while blocked.

This is the "throttle based on specific failures as they emerge" from §4.3.
It is not a global request limiter; GETs are not gated.

Blast radius: `GitHub` gains two methods and one attribute; `arequest` gets an
optional hook; three call sites gain one `await`.

### C3 — retry-wrap `create_github_sibling()` (keep DataLad) (`adataset.py`, `zarr.py`, `datasetter.py`)

* Catch `IncompleteResultsError`; if any `e.failed[*]["message"]` mentions a
  rate limit / abuse detection / "blocked from content creation", call
  `note_rate_limited(None)` (DataLad does not surface `Retry-After`), await
  the cooldown, and retry — bounded (e.g. 10 attempts, ≥ 60 s, exponential,
  capped per attempt).  Any other `IncompleteResultsError` is re-raised
  unchanged.  Safe because a 403 means the repo was not created, and
  `existing="reconfigure"` already handles "created but not configured".
* Serialise repo creation through a single `anyio.Lock` held by `GitHub`,
  with a minimum spacing (~1 s) between creations — GitHub's own guidance for
  content-creating requests is "serially, ≥ 1 s apart".  This narrows the
  burst from 10 concurrent to 1 in flight; it does not slow anything else
  (downloads keep running under `zarr_limit`).
* Pass `description=` to `create_sibling_github` so repos are never born as
  `some default`.  At creation time file counts are unknown, so use an
  interim string such as `Zarr <id> of Dandiset <nnnnnn> (backup in
  progress)`; `set_zarr_description()` replaces it once stats exist.

The `GitHub` instance (`manager.gh`) is available at both call sites; pass it
(or just the lock + cooldown) into `create_github_sibling()` as an optional
argument so `AsyncDataset` stays free of `Manager`.

Blast radius: `create_github_sibling()` gains a retry loop and two optional
parameters; two call sites pass them.

### C4 — make the Zarr GitHub state converge on every visit (`zarr.py`, `manager.py`)

This is what actually fixes half-brewed repositories, and what lets a
rate-limited run be simply re-run.

* **Push when behind, not only when we just committed.**  Replace the gate at
  `zarr.py:607` with `made_commit or behind`, where `behind` is "the `github`
  remote exists and `HEAD != refs/remotes/github/draft`" (the remote-tracking
  ref is updated by every successful push, and is absent for a never-pushed
  sibling).  One local `git rev-parse` per visit; no API call.
* **Describe when never described.**  Replace the gate at `zarr.py:625` with
  `made_commit or FORCE or <no cached dandi.github-description>`.
  `_set_github_description()` already PATCHes only when the computed string
  differs from the cache, so already-described Zarrs still cost zero API calls
  on no-op runs.  (The Dandiset-side gate at `datasetter.py:247-259` exists to
  avoid an unconditional `GET /repos` per Dandiset; the Zarr path has no such
  GET, so relaxing it is free.)
* **Fix F5.**  Let `set_zarr_description()` take the `AsyncDataset` that is
  actually on disk (the `sync_zarr()` caller has it) instead of rebuilding
  `zarr_root / zarr_id`; keep the id-based form for `update_github_metadata()`
  and assert `ds.ds.is_installed()` so the global-config fallback can never
  happen silently again.

Not touched: the Dandiset flow (`datasetter.update_dandiset`).  The same
"push when behind" idea applies there, but it has not misbehaved and is out of
scope for this branch (see §9).

### C5 — sample GitHub's rate-limit report (`manager.py`, `datasetter.py`)

* `GitHub.get_rate_limit()` → `GET /rate_limit` (does not count against the
  quota).  Log `core.remaining/limit` and `reset` at INFO at the start of
  `update-from-backup` / `backup-zarrs`, whenever C1/C3 record a rate-limit
  event, and periodically (e.g. every 15 min) while a run lasts.
* Optionally: if `core.remaining` is below a small threshold, treat it as a
  cooldown until `reset` (C2) before content creation.

Honest caveat: `/rate_limit` reports the *primary* per-hour budgets only.  The
secondary "content creation" limit that hit us is not observable through any
endpoint; the only signal is the 403/429 itself.  So C5 is diagnostics plus
protection against the primary budget, not a substitute for C1–C3.

### C6 — protect the remaining content-creating calls

* `create_release()` (`manager.py:156`): go through C1's classification and
  await the C2 cooldown (no change to its non-rate-limit behaviour).
* Tag push at `datasetter.py:550`: await the cooldown first.  (Git-over-HTTPS
  pushes are throttled differently, but a blocked token can affect them too.)

### C7 — reconcile what is already broken on `dandizarrs`

Reuse `update-github-metadata` (documented as the out-of-band refresh, and it
already walks every Dandiset's Zarr subdatasets and calls
`set_zarr_description()`, which PATCHes wherever the local cache is missing
or stale — i.e. it already repairs `some default` once C4/F5 land):

* add `--push-behind` (Zarrs, optionally Dandisets): push where
  `HEAD != refs/remotes/github/draft` (same predicate as C4);
* add `--report-orphans`: page `GET /orgs/{zarr_gh_org}/repos` and list repos
  with no local dataset / with `description == "some default"` / not pushed
  (`size == 0`).  Report only; deletion stays manual.
* everything above goes through C1/C2, and stops cleanly when a rate limit
  persists, so it can be re-run.

### C8 — (deferred) isolate Zarr failures inside a Dandiset sync

Catch per-Zarr exceptions in the nursery task, let the other Zarrs finish,
then raise a summary — like `pool_amap()` does for Dandisets.  With C4 in
place cancellation no longer *loses* anything (the next visit heals), so this
becomes a throughput/cleanliness improvement rather than a correctness fix,
and it changes semantics in `asyncer.py` that have been stable.  Proposed as a
follow-up, not part of this branch.

## 7. Rollout

1. Land C1–C4 (+ C6) in one PR.  Behaviour of a run that never hits a limit is
   unchanged except: repo creation is serialised with ~1 s spacing, new repos
   get an interim description, and never-pushed / never-described Zarrs get
   pushed / described on their next visit.
2. Run `update-github-metadata --push-behind --report-orphans` (C7) once
   against `dandizarrs`, budget-limited, possibly over several invocations.
3. Then the usual cron.  A run that hits the content-creation limit now backs
   off collectively, resumes, and if the hourly budget is truly exhausted,
   fails only the Zarrs it could not create — which the next run picks up.

Operational note: the content-creation limit is documented (at the time of
writing; verify against current docs) as ~80 requests/minute and 500/hour per
token.  Standing up thousands of Zarr repositories therefore takes hours no
matter what we do; the point of this plan is that it makes progress across
runs instead of leaving debris.

## 8. Tests (`@pytest.mark.ai_generated` where AI-written)

* `test_aioutil.py`: `arequest()` against a mocked `httpx` transport — 403
  with `Retry-After`, 403 with `x-ratelimit-remaining: 0` + reset, 429, a
  plain 403 (must *not* be retried unless in `retry_on`), and the sleep caps.
* `GitHub` cooldown: two concurrent callers, one trips the limit, the other
  waits; no-op when nothing tripped.
* `create_github_sibling()` retry: monkeypatch `ds.create_sibling_github` to
  raise `IncompleteResultsError` with a rate-limit message once, then succeed;
  a non-rate-limit `IncompleteResultsError` propagates immediately.
* `sync_zarr()` convergence: a Zarr with a `github` remote and no remote
  tracking ref gets pushed on a no-change visit; a Zarr with no cached
  description gets described; both are no-ops once converged (assert no
  API call via a mocked `GitHub`).
* Regression for F5: `set_zarr_description()` writes the cache into the
  dataset that is on disk.

## 9. Open questions for maintainers

1. **F7 (dirty Zarr after interruption).**  Leave the hard error as is, or let
   `sync_zarr()` recover automatically when the only dirt is under Zarr content
   paths (which `ZarrSyncer` is designed to reconcile against S3 anyway)?
2. **Dandiset-side push-when-behind.**  Apply the C4 predicate to
   `datasetter.update_dandiset()` too, or keep the Dandiset flow untouched
   on this branch?
3. **Serialising repo creation (C3).**  Is one-at-a-time with ~1 s spacing
   acceptable, or should it be a small `CapacityLimiter` (2–3)?  GitHub's
   guidance argues for 1.
4. **Interim description text** for freshly created repos (C3).
5. **Where should the cooldown/lock live** — on `GitHub` (proposed; already
   one per process) or on `BackupConfig` next to `zarr_limit`?

## 10. Explicitly out of scope

* Reimplementing repo creation outside DataLad.
* A global request limiter / pacer for all GitHub traffic.
* Switching from a PAT to a GitHub App installation token (higher primary
  budget; the secondary content-creation limit still applies).
* Changes to asset download / annex code paths.
