# Plan: surviving GitHub secondary rate limits when creating Zarr repositories

Status: **v5 — implemented** (2026-09-16).  Comment inline on the PR, or edit
this file directly.  §2–§3 describe `main` *before* this PR (line references
against `2e85461`); the items marked done below supersede them.

Implementation status (agreed by three independent senior-developer reviews
as the minimal, non-refactoring cut):

* **Done in this PR**: C1 (rate-limit-aware `arequest()`, opt-in via a
  `GitHubGate`), C2 (one per-process gate: cooldown from GitHub's headers or
  its documented fallback, ≥ 1 s spacing, give-up once
  `GITHUB_RATE_LIMIT_ATTEMPTS` consecutive hits have been slept out), C3 (retry-wrapped
  `create_github_sibling()`, both DataLad failure shapes, `description=`,
  idempotent sibling config), C4 (push unpushed commits, describe when never
  described, F5 fix), C6 (`create_release` through the gate), and the
  dirty-dataset digest from C5.
* **Deliberately not done**: cache seeding / interim text (a fresh Zarr is
  described on the visit that creates it; "cache missing" is the whole
  predicate), the cumulative-time breaker and per-run mutation budget (no
  local quota numbers), `/rate_limit` sampling, the identity check, the
  end-of-run summary, idempotent `initremote`, creation-after-commit, C7
  `reconcile-zarrs`, C8.  Debris that only a reconcile pass can reach (§3.5,
  G1/G2) remains a follow-up.

v2 folded in three independent reviews (fact-check, design, ops/recovery) of
v1; v3 applied the maintainer's decisions on dirty datasets and cron
locking; v4 removes every locally invented quota number and makes explicit
that pushes are never forced.  §12 lists what changed.  Everything cited as `file:line` was
re-verified against `main` (2e85461) and DataLad `38368d7`.

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

The two lines come from *different* `create_sibling_github` calls: they run in
worker threads (`anyio.to_thread.run_sync`, `adataset.py:765`) and DataLad's
default result renderer prints each record to stdout as it is yielded, so
records from concurrent calls interleave.  The records carry no path
(`create_sibling_ghlike.py:630-637`), so the `(error)` one could be a Zarr or
a Dandiset creation — both go through the same `create_github_sibling()`.

## 2. What the code actually does today

### 2.1 Calls that talk to GitHub

| Operation                              | Where                                                           | Retry / throttle today                         |
| -------------------------------------- | --------------------------------------------------------------- | ---------------------------------------------- |
| `POST /orgs/{org}/repos` (create repo) | DataLad `create_sibling_github` (thread), via `adataset.py:751` | none: plain `requests`, no timeout             |
| `PATCH /repos/…`                       | `GitHub.edit_repo()`, `manager.py:146`                          | `arequest(retry_on=[403, 404])`                |
| `GET /repos/…`                         | `GitHub.get_repo()`, `manager.py:132`                           | `arequest(retry_on=[403])`                     |
| `POST /repos/…/releases`               | `GitHub.create_release()`, `manager.py:156`                     | transport errors / 5xx only; 403 and 429 fatal |
| `datalad push`                         | `AsyncDataset.push()`, `adataset.py:477`                        | `"unexpected disconnect"` only [a]             |
| `git push github <tag>`                | `datasetter.py:550`                                             | none; SSH `pushurl`, so not REST-limited       |

[a] A rejected ref surfaces as `IncompleteResultsError`, which `push()` does
not retry (it catches only `CommandError`).

Call sites of repo creation: `DandiDatasetter.ensure_github_remote()`
(`datasetter.py:180`, Dandisets — followed immediately by a `homepage` PATCH,
`:187-190`) and `sync_zarr()` (`zarr.py:559`, Zarrs).

Other mutating paths, for completeness (none is part of the incident, all go
through `edit_repo`/`push` and would inherit the changes below):
unembargo → `edit_repo(private=False)` plus one PATCH per Zarr
(`syncer.py:114-120`, `:261-273`); deleted Dandiset → `edit_repo(private=True)`
(`datasetter.py:132-135`); superdataset description (`datasetter.py:375`);
extra push after the release merge (`datasetter.py:421-427`); `release`
(`__main__.py:380`) and `populate` (`__main__.py:599`) pushes;
`update-github-metadata` = GET + PATCH per Dandiset and PATCH per Zarr with a
missing/stale cache (`datasetter.py:319-338`).  Each `datalad push` is 3-4
round trips (dry-run push, `fetch git-annex`, push; the first push is split
in two by `datalad-push-default-first`).  Freshly pushed Zarr repos are then
cloned from github.com into the Dandiset (`asyncer.py:565-574`,
`datasetter.py:613-627`), in threads, without retry.

Concurrency feeding the burst: `config.zarr_limit` is one process-wide
`CapacityLimiter(ZARR_LIMIT = 10)` (`consts.py:16`, `config.py:121-123`,
acquired at `zarr.py:512`), so up to 10 `sync_zarr()` run at once regardless
of `workers`; Dandiset creations add up to `DEFAULT_WORKERS = 5` more.  Repo
creation happens *before* the content sync (`zarr.py:559` vs `:576-586`), so
a batch of new Zarrs fires all its creations as soon as slots free up.

### 2.2 What DataLad does on a 403 (`datalad/distributed/create_sibling_ghlike.py`)

* `repo_create_request()` POSTs with plain `requests`, no timeout
  (`:494-499`).
* `repo_create_response()` maps **401/403** to a result record
  `status='error', message=('unauthorized: %s', <API message>)` (`:549-554`)
  and **500** to an error record (`:555-559`).  **Everything else — including
  429, which GitHub also uses for secondary limits — hits
  `r.raise_for_status()` (`:561`) and propagates as
  `requests.exceptions.HTTPError`** out of the thread.  The same applies to
  the `existing='reconfigure'` GET (`repo_get_request`, `:358-368`).
* For the error record, `create_repos()` `continue`s without configuring the
  local sibling (`:427-429`), and `@eval_results` raises
  `IncompleteResultsError` with the record in `e.failed` (`base.py:945-948`).
  The message is a `(fmt, arg)` tuple; `Retry-After` and the status code are
  not preserved.
* 422 "already exists" + `existing='reconfigure'` → `repo_get_request()` →
  `status='notneeded'` and the sibling *is* configured (`:293-308`,
  `:434-459`).  Re-running after "repo created but sibling not configured" is
  therefore already safe.
* `desc_text = description if description is not None else 'some default'`
  (`:478`).  `create_github_sibling()` never passes `description=`
  (`adataset.py:766-777`), so **every repository we create is born with the
  description `some default`** and only gets a real one later from
  `set_zarr_description()` / `set_dandiset_description()`.
* DataLad authenticates with its own credential (`Token("api.github.com")`
  from DataLad's credential store, `:175-196`), not with the `GITHUB_TOKEN` /
  `hub.oauthtoken` our `GitHub` client uses (`datasetter.py:60-71`).  Whether
  the two are the same account is a deployment fact, not a code fact.

Consequences: a 403 on creation means the repository was *not* created
(retrying is idempotent); the failure surfaces either as
`IncompleteResultsError` (403, message text only) or as `requests.HTTPError`
(429 and the reconfigure GET; headers available).

### 2.3 Our own retry helper

`arequest()` (`aioutil.py:119`) retries on `httpx.RequestError`/`SSLError`,
5xx, and any status in `retry_on`, sleeping `exp_wait(attempts=15, base=2)`
(`:126`): 1, 2, 4, … 16384 s (±5 % jitter, `util.py:271`) — up to ~4.5 h for
one sleep and ~9.1 h in total, a budget that exists to ride out DANDI API
outages (`:127-128`).  It reads `Retry-After` / `x-ratelimit-*` for logging
only (`_describe_http_error`, `:184-199`), never for sleeping; does not retry
429 anywhere; and cannot tell a secondary-rate-limit 403 from a bad-token 403.
**It also serves the DANDI API (`adandi.py:61`) and S3 `HEAD` (`blob.py:43`)**,
so any change to its policy is not GitHub-specific.

### 2.4 GitHub's documented semantics (github/docs source, 2026-09-14)

Confident: secondary-limit responses are **403 or 429**; honour `retry-after`
*if present*; if `x-ratelimit-remaining` is 0 wait until `x-ratelimit-reset`;
otherwise wait ≥ 1 min, then exponentially, and give up after a fixed number
of retries; "continuing to make requests while you are rate limited may result
in the banning of your integration"; content creation is "in general" ≤ 80/min
and ≤ 500/h, some endpoints lower, "subject to change without notice", and
limits can also trigger "for undisclosed reasons"; **there is no way to check
the status of a secondary limit** — `GET /rate_limit` reports primary budgets
only, does not count against the primary limit, *but can count against the
secondary one*; mutating requests cost 5 points against a 900 points/min
per-endpoint budget; ≤ 100 concurrent requests; "wait at least one second
between each" POST/PATCH/PUT/DELETE when making many.  The limits are per
**user account** (all tokens, apps, web UI).

Not documented: how long a content-creation block lasts; whether it carries
`retry-after`; whether repo creation is one of the "lower limit" endpoints.
Do not encode 80/500 in logic; log headers and react to them.

### 2.5 How GitHub informs us today, and what that leaves for local constants

* Every REST response carries `x-ratelimit-limit/remaining/used/reset/resource`
  — the *primary* budget for that resource — and a limit response may carry
  `retry-after`.  `GET /rate_limit` reports the same primary numbers.
* We read those headers **for logging only** (`_describe_http_error()`,
  `aioutil.py:184-199`); nothing acts on them, and the sleep schedule is our
  own `exp_wait` (§2.3).  DataLad's call does not surface them at all.
* The *secondary* content-creation limit that hit us publishes nothing beyond
  the 403/429 itself and, if present, `retry-after`: no header, no endpoint,
  no quota to read (`x-ratelimit-remaining` is normally still > 0 on such a
  response).  Its thresholds exist only in prose and change without notice.

So the plan relays GitHub's signals — `retry-after`, `x-ratelimit-reset`,
and its documented fallback for an unannounced secondary limit (≥ 60 s,
doubling) — and encodes **no per-hour or per-run budget of its own**.  The
only local numbers are: how many consecutive rate-limited responses before
the run gives up on further creations (GitHub: "throw an error after a
specific number of retries"; the number is ours), the ≥ 1 s spacing between
mutations (GitHub's), and a timeout for DataLad's untimed POST (ours, not a
quota).

## 3. Fragilities this exposes

* **F1 — the DataLad creation call has no retry of any kind**, and it is the
  burstiest content-creating call we make.  `create_release()` retries
  transport errors only; neither handles a rate-limit response.
* **F2 — the block is per account, not per request.**  Once one worker trips
  it, every worker is blocked; independent per-call retries do not model
  that and, per GitHub, retrying while blocked can extend it.  A *shared*
  cooldown does.  Caveat: if DataLad's credential and `GITHUB_TOKEN` are
  different accounts, a block seen through one says nothing about the other.
* **F3 — one failed Zarr cancels its siblings.**  `sync_zarr()` is
  `start_soon`'d into the Dandiset's asset nursery (`asyncer.py:358`; task
  group at `:516`); the exception cancels every in-flight Zarr *and* blob
  download for that Dandiset.  Threads already running (create, push, clone)
  finish first (anyio's default `abandon_on_cancel=False`), the cancellation
  lands at the next checkpoint.
* **F4 — "needs push" and "needs description" are not durable state.**  Push
  is gated on `made_commit` alone (`zarr.py:588-589`, push at `:606-613`);
  description on `made_commit or ZarrMode.FORCE` (`:624-626`).  A
  committed-but-unpushed Zarr is therefore **never pushed by any existing
  command**: `--zarr-mode force` only bypasses `needs_sync()` (`:354`) and
  re-walking unchanged content adds nothing to the report (`:224-229`), so
  `made_commit` stays `False`.  FORCE does re-describe.  (Dandisets use the
  same pattern at `datasetter.py:239-260`; out of scope here.)
* **F5 — `set_zarr_description()` crashes under `backup-zarrs`.**
  `manager.py:113` builds `AsyncDataset(config.zarr_root / zarr_id)`, but
  `backup_zarrs()` syncs in `partial_dir / zarr_id` and moves afterwards
  (`datasetter.py:579,596`).  For a non-existent path DataLad's
  `Dataset.config` is the *global* manager (`dataset.py:326-335`), whose
  commands run as `git --git-dir=/dev/null config` (`config.py:494-496`);
  `scope="local"` appends `--local` (`:1032`) and git refuses it
  (rc 128, "--local can only be used inside a git repository").  Sequence:
  PATCH sent (`manager.py:52`), commit and push done (`zarr.py:599-613`),
  then `CommandError` at `manager.py:57` → `dobackup()` aborts before
  `shutil.move` (`datasetter.py:596`) → **the Zarr is stranded in
  `partial_dir`**.  Present since `bb91ccd` (2024-01-30); untested because
  `test_backup_zarrs` has no `github_org` and `test_zarrbargo` mocks the
  method.  The `update-from-backup` path passes the real directory
  (`asyncer.py:352-353`) and is unaffected.
* **F6 — `create_release()` is unprotected** (`manager.py:156-160`); a fresh
  mirror of a Dandiset with many published versions is another burst
  (`datasetter.py:389-395`, serial per Dandiset).
* **F7 — an interrupted Zarr sync leaves a dirty dataset that blocks every
  later visit.**  `sync_zarr()` raises `"Zarr … is dirty; clean or save
  before running"` (`zarr.py:566-570`) before `ZarrSyncer` gets to converge.
  **Decision (maintainer): keep the hard error.**  Automated mitigation
  (stash / `git reset --hard` / `git clean -dfx`) is a separate feature; what
  this plan adds is a *report* that makes the manual reset easy (C5).
* **F8 — a failed Zarr leaves its *Dandiset* dirty, so every later cron run
  fails on that Dandiset until someone cleans it by hand.**  `async_assets()`
  runs `tracker.dump()` in a `finally:` (`asyncer.py:532-533`) but the
  `ds.add(".dandi/assets.json")` after it (`:535`) is skipped; `finish_asset()`
  ran before the Zarr sync started (`:341`) so `assets.json` differs from
  HEAD; `dandiset.yaml` is already staged (`datasetter.py:294`); blobs
  `addurl`'d so far are staged.  The state timestamp is not advanced, so the
  Dandiset is re-selected (`datasetter.py:210-214`) and hits `Dirty …`
  (`:273-274`) → `pool_amap` marks it failed → exit 1, every run, and
  `set_superds_description` is skipped.  Pre-existing and not specific to
  rate limits (any download failure does the same via `Report.check()`,
  `asyncer.py:86-100`); F3 makes it far more likely.  Same decision as F7:
  not auto-mitigated here, reported.
* **F9 — a cancelled `create_github_sibling()` leaves a sibling that is never
  repaired.**  The thread completes (repo created *and* `siblings configure`
  run), the cancellation lands before our `set_repo_config` for
  `remote.github.pushurl`, `branch.draft.remote/merge` (`adataset.py:779-784`);
  every later visit takes the `has_github_remote()` branch (`:760`, `:786-790`)
  and never sets them.  For public Zarrs DataLad configures only the HTTPS
  `clone_url` (`create_sibling_ghlike.py:450-454`), so a later push goes over
  HTTPS with no `pushurl`.  Same shape for `ensure_installed()`: created but
  cancelled before `initremote` (`adataset.py:110-130`) → `is_installed()`
  short-circuits forever (`:77-83`) and `get_keys_missing_from()` fails every
  visit (`annex.py:111-115`).

### 3.5 Debris matrix — what the incident can leave behind, and what heals it

Local states under `zarr_root/<id>` (`update-from-backup`) or
`partial_dir/<id>` (`backup-zarrs`; only `backup-zarrs <same Dandiset>` ever
re-enters it, and `ultimate_dspath.exists()` (`datasetter.py:575-578`) skips
anything already moved):

| #   | State                                                         | Next plain run today                            | After this plan                    | Manual?                            |
| --- | ------------------------------------------------------------- | ----------------------------------------------- | ---------------------------------- | ---------------------------------- |
| S1  | `datalad create` interrupted; no valid repo                   | `create` refuses the non-empty dir, every visit | same                               | `rm -rf`                           |
| S2  | created, `initremote` not run (F9)                            | `get_keys_missing_from` raises, every visit     | same (follow-up)                   | `git annex initremote` by hand [a] |
| S3  | `.gitattributes` / embargo config written, commit cancelled   | dirty → F7 error, every visit                   | same error, now with a digest (C5) | reset by hand                      |
| S4  | sibling configured, our `pushurl` / `branch.*` unset (F9)     | pushes over HTTPS without `pushurl`             | C4: sibling config idempotent      | —                                  |
| S5  | `ZarrSyncer` cancelled → dirty worktree                       | F7 error, every visit                           | same, reported (C5)                | reset by hand                      |
| S6  | committed, push cancelled                                     | **never pushed** (F4) [b]                       | C4 pushes on the next visit [b]    | —                                  |
| S7  | pushed, never described (`some default`)                      | never (F4)                                      | C4 or C7                           | —                                  |
| S8  | Zarr complete, Dandiset failed → no submodule                 | healed by the clone path once D1 is fixed       | same                               | fix D1 first                       |
| D1  | Dandiset dirty (F8)                                           | fails every run                                 | same error, now with a digest (C5) | **yes**: §7 step 0                 |
| G1  | GitHub repo with no local dataset                             | invisible                                       | C7 `--orphans` report              | delete by hand                     |
| G2  | repo deleted on GitHub; local tracking ref still matches HEAD | not re-created, not pushed                      | C7 checks `git ls-remote` [c]      | or drop the remote [c]             |

[a] Idempotent `initremote` in `ensure_installed()` is a follow-up.
[b] Today the Dandiset then clones an *empty* repo, `submodule add` fails and
the Dandiset is left dirty (F8).  C4 pushes inside `sync_zarr()`, before the
clone.
[c] C4 alone cannot tell — the tracking ref lies.  `reconcile-zarrs` verifies
with `git ls-remote --exit-code github`; alternatively `git remote remove
github` in `zarr_root/<id>` so the next visit recreates, pushes, describes.

"Next visit" means: the Zarr's Dandiset is re-synced (timestamp advanced, or
`--mode force`) and the Zarr asset is encountered (`datasetter.py:210-230`).
For the incident that holds — the failed Dandisets never advanced their
timestamp — but a Zarr whose Dandiset is already up to date is not revisited
by cron at all; that is what C7 is for.

## 4. Constraints for the fix (maintainer feedback)

1. Keep using DataLad's `create_sibling_github`; do not reimplement repo
   creation.
2. The system has been very robust; do not disturb unrelated code paths.  The
   Dandiset-side flow stays as is unless a change is clearly needed there.
3. Prefer *reactive* throttling — react to the specific failures as they
   emerge, plus occasional sampling of GitHub's rate-limit report — over a
   proactive choke point for all traffic.  Reuse an existing gate if one fits.

## 5. Existing gates, and what they cover

| Gate                                        | Covers                      | Notes                                                        |
| ------------------------------------------- | --------------------------- | ------------------------------------------------------------ |
| `config.zarr_limit` = `CapacityLimiter(10)` | whole `sync_zarr()` body    | only bound on concurrent creation; also bounds content syncs |
| `config.workers` = 5                        | Dandisets in parallel       | same                                                         |
| `Manager.gh`: one `GitHub` per process      | all `manager.py` API calls  | shared by every worker [a]                                   |
| `arequest()`                                | `manager.py`, DANDI API, S3 | shared with non-GitHub callers: GitHub policy must be opt-in |
| `AsyncDataset.push()` retry loop            | pushes                      | `"unexpected disconnect"` only                               |

[a] `Manager.with_sublogger` is `dataclasses.replace` (`manager.py:36-37`),
so `gh` and `config` are the same objects everywhere (`datasetter.py:57-82`)
— the natural owner of per-process "GitHub is cooling down" state.

Nothing covers the DataLad call, and nothing is shared between workers once a
block is in effect.  There is no gate to reuse for content creation; the
proposal adds the narrowest one that is coherent — a per-process gate on
`GitHub` for *mutations* that is inert until GitHub complains, except for the
≥ 1 s spacing GitHub itself asks for — not a limiter on all traffic.

## 6. Proposed changes

In one paragraph: serialise GitHub mutations behind one lock with ≥ 1 s
spacing; on a rate-limit response sleep for what GitHub says (or its
documented fallback) and give up on further creations for this run after N
consecutive ones; wrap DataLad's repo creation in that, passing a real
description; on every visit push commits the remote lacks (a plain push,
never forced) and describe repos still undescribed; fix F5; report dirty
datasets.  Everything else below is diagnostics or cleanup tooling.

Staged into three PRs (§7).  Each item names its blast radius; §7.1 lists
every behavioural delta for runs that never hit a limit.

### C1 — rate-limit awareness for GitHub requests, opt-in (`aioutil.py`, `manager.py`)

* `arequest()` gains an optional `rate_limiter` argument (the gate from C2).
  **Without it, behaviour is byte-for-byte today's** — DANDI API and S3
  callers are untouched, including the 9 h 5xx budget.
* With it: a 403/429 whose body mentions a secondary/abuse rate limit, or
  that carries `retry-after`, or `x-ratelimit-remaining: 0`, is classified as
  a rate-limit response (unmatched 403 bodies are logged at WARNING so the
  classifier can be extended); `retry_on` semantics stay for everything
  else (`get_repo`/`edit_repo` keep their plain-403 retry from `80725f2`).
* The sleep for a rate-limit response is computed by the gate (C2), *not*
  added on top of `exp_wait`; 429 becomes retryable for GitHub (today it is
  fatal for `edit_repo`, `get_repo`, `create_release`).
* `_describe_http_error()` additionally records `x-github-request-id`
  (needed for GitHub support tickets) and `x-ratelimit-used/limit`.

### C2 — one per-process gate on `GitHub` (`manager.py`)

A small `GitHubGate` (in `aioutil.py`, so `adataset.py` can receive it
without importing `manager`), held by `GitHub.gate` — one per credential and
process, shared by every worker through `Manager.with_sublogger`.  All state
is touched only from the event loop (never from inside a DataLad thread).  It
encodes no GitHub quota numbers; it relays what GitHub says (§2.5):

* **Cooldown**: `note_rate_limited(headers, what)` sets `cooldown_until =
  max(cooldown_until, now + delay)` with `delay` = `retry-after` if present
  (clamped to ≥ 1 s; GitHub may say 0); else until `x-ratelimit-reset` if
  `x-ratelimit-remaining` is 0; else GitHub's documented fallback for an
  unannounced secondary limit: 60 s, doubling per consecutive hit.  A hit
  that arrives while a cooldown is still running does not escalate (ten
  workers failing in the same second are *one* incident).  `wait()` loops
  until `cooldown_until` has passed.
* **Give-up**: `GITHUB_RATE_LIMIT_ATTEMPTS` (5) consecutive rate-limited
  responses are each slept out and retried; on the next one the gate gives
  up for the rest of the process: mutations raise `GitHubRateLimited`
  immediately (so the run ends with N failed Zarrs instead of every worker
  sleeping in turn), and reads fall back to `arequest`'s ordinary bounded
  retry policy instead of looping on the cooldown.  Any successful mutation
  resets the counter; reads never reset it (but a rate-limited read does
  count).  With the doubling fallback, 5 slept-out hits bound the wait at
  ~31 min; a `retry-after` from GitHub is honoured as given.
* **Serialisation + spacing**: an `anyio.Lock` plus ≥ 1 s between the *end*
  of one mutation and the start of the next — GitHub's own guidance — for
  repo creation, `edit_repo` and `create_release` alike.  This is what turns
  a cooldown expiry into one prober instead of ten simultaneous retries.
  Inside `arequest` the lock is taken per attempt around the request only, so
  5xx/404 backoff sleeps run unlocked; the creation retry loop (C3) holds it
  across its attempts.  Lock order is always `zarr_limit` → gate; nothing
  acquires `zarr_limit` under the gate; `backup_zarrs`' `dslock` is taken
  only after `sync_zarr` returns → no deadlock.
* The four numbers (`GITHUB_RATE_LIMIT_ATTEMPTS`, `GITHUB_MUTATION_SPACING`,
  `GITHUB_RATE_LIMIT_FALLBACK`, `GITHUB_CREATE_TIMEOUT`) live in
  `consts.py`; no YAML/CLI/environment knobs, and they are not logged at
  startup.

Known, accepted limitation: a Zarr sleeping in the cooldown holds its
`zarr_limit` slot (`zarr.py:512`) and its Dandiset worker, so during a
cooldown *all* Zarr work in the process stalls.  Restructuring that is out of
scope; the give-up bounds it.  Likewise a DataLad thread abandoned on
timeout (C3) may still finish creating/configuring the sibling outside the
gate; `existing="reconfigure"` and the config restore absorb that on the
next visit.

### C3 — retry-wrap `create_github_sibling()` (keep DataLad) (`adataset.py`, `zarr.py`, `datasetter.py`)

* Call `create_sibling_github(..., description=..., result_renderer="disabled",
  on_failure="ignore", return_type="list")` and inspect the result records
  ourselves: no more interleaved stdout; ok/notneeded records go to our
  logger at DEBUG, tagged `owner/name`.  DataLad itself still logs each
  failed attempt's message at ERROR through its own logger, so a retried
  (and recovered) rate-limit hit shows up once as a `datalad` ERROR line next
  to our `RATELIMIT` warning.
* Classify **both** shapes: an `error` record whose formatted `message` (a
  `(fmt, args)` tuple) mentions a rate limit / abuse detection / "blocked
  from content creation", **and** `requests.HTTPError` with status 403/429
  (this one carries `retry-after`; DataLad's record does not, so that shape
  always uses the 60 s fallback).  Any other failure is re-raised unchanged
  (a non-rate-limit error record becomes a `RuntimeError`, without retry).
* Retry loop: hold the gate's lock across the *whole* loop (one prober),
  each sleep computed by the gate; once the gate has given up, raise
  `GitHubRateLimited`.  Each attempt runs under `anyio.move_on_after(
  GITHUB_CREATE_TIMEOUT)` with `run_sync(..., abandon_on_cancel=True)` so a
  hung `requests.post` (no timeout in DataLad) cannot pin the lock or block
  cancellation; a timeout is a `RuntimeError`, and the rerun is safe via
  `existing="reconfigure"`.
* Pass a real `description=` — `Zarr <id> of Dandiset <nnnnnn>` /
  `Dandiset <nnnnnn>` — so repositories are never born as `some default`.
  No interim text and no cache seeding: a fresh Zarr is described on the
  visit that creates it, and "cache missing" is the whole describe predicate
  (C4).
* Make our post-creation local config idempotent (F9/S4): on the
  existing-remote path, `remote.github.pushurl` and
  `branch.draft.remote/merge` are set whenever they are missing.

### C4 — make the Zarr GitHub state converge on every visit (`zarr.py`, `manager.py`, `adataset.py`)

This is what fixes S4, S6 and S7 and lets a rate-limited run simply be
re-run for the Zarr side.

* **Push unpushed commits — a plain push, never forced.**  Replace the
  `made_commit` push gate with `made_commit or unpushed`, computed locally by
  `AsyncDataset.has_unpushed_commits()`: resolve the upstream from
  `branch.<current>.remote/.merge` (set at creation; do not hardcode
  `draft`); no upstream → warn, don't push; tracking ref absent → push; else
  push iff `git merge-base --is-ancestor HEAD <tracking>` is false, i.e. HEAD
  has commits the remote lacks.  If the remote has diverged, the plain push
  is rejected and the Zarr fails as it does today; `--force` is passed only
  under `--force-push`, exactly as now (and see §11: that flag currently
  does not force at all).  The git-annex branch is deliberately excluded (it
  is always ahead — `backup_zarrs` runs `annex describe here` after the
  push).  Caveat G2: a stale tracking ref after a manual deletion on GitHub
  is a follow-up's job.
* **Describe when not yet described.**  Gate: `made_commit or FORCE or the
  dandi.github-description cache is missing`, read through the same
  `ds.ds.config` that `_set_github_description()` writes.
  `_set_github_description()` PATCHes only on a difference, so converged
  Zarrs cost zero API calls on no-op runs.  A `GitHubRateLimited` from the
  describe step is logged and swallowed: the content is already committed and
  pushed, the cache stays unset, and the next visit retries — a description
  is not worth failing the Zarr (and dirtying its Dandiset, F8) over.  The
  Zarr path has no `GET /repos` (the Dandiset gate at `datasetter.py:247-259`
  exists for that GET).  First-run backlog: every `backup-zarrs` Zarr (F5),
  every Zarr from before `bb91ccd`, and every failed PATCH lacks a cache →
  one PATCH plus a `get_stats()` walk each, paced at 1/s by the gate.
* **Fix F5.**  `set_zarr_description()` takes the `AsyncDataset` actually on
  disk (the caller has it); the id-based form kept for
  `update_github_metadata()` raises unless the dataset is installed, so the
  global-config fallback can never happen silently again.

Not touched: `datasetter.update_dandiset()` (§9 Q1); idempotent `initremote`
in `ensure_installed()` (S2) is a follow-up.

### C5 — diagnostics: dirty-dataset digest (`adataset.py`, `zarr.py`, `datasetter.py`)

Implemented: **dirty-dataset digest** only.  `AsyncDataset.describe_dirt()`
returns the number of dirty paths and the first 10 lines of `git status
--porcelain` (`... and N more`); the `is dirty; clean or save before running`
errors at `zarr.py` and `datasetter.py` carry it, so the operator's reset list
is readable from the `Job failed on input …` traceback in the log.  Read-only:
no stash, reset or clean.

Not implemented (follow-ups): `GET /rate_limit` sampling (primary budgets
only, and it can itself count against the secondary limit), the
`GITHUB_TOKEN`-vs-DataLad-credential identity check, per-status/area grouping
in the digest, per-Zarr outcome lines and an end-of-run `DIRTY` summary.

### C6 — `create_release()` through the gate

Classification via C1, cooldown/lock/spacing via C2.  The tag push at
`datasetter.py:550` stays as is: it uses the SSH `pushurl`, which REST blocks
cannot reach.

### C7 — `reconcile-zarrs`: repair what is already broken

A new, small command (report-only by default, `--fix` to act; `--limit N`)
that walks `zarr_root/*` (and `--partial-dir`, like `populate_zarrs`,
`__main__.py:462-511`) rather than Dandisets' submodules — the incident's
Zarrs never became submodules, so `update-github-metadata`
(`datasetter.py:319-338`) cannot reach them, and it also does one
`GET /repos` per Dandiset, the burst `b49b356` removed from cron.  Per local
Zarr: installed? special remotes present? `github` remote + `pushurl` +
`branch.*` set? repo exists (`git ls-remote --exit-code github`, git-over-SSH,
not REST)? unpushed commits (C4 predicate)? description cache missing? dirty?
With `--fix`: apply C3/C4's idempotent setup, push, describe — all through
the gate, with `--limit N`, so it can be re-run until clean.
`--orphans`: page `GET /orgs/{zarr_gh_org}/repos` (`Link` pagination; GETs,
cooldown-observed) against local dirs and every Dandiset's `.gitmodules`;
"nothing pushed" = `GET /repos/{o}/{r}/branches` returns `[]` (repo `size` is
KB-rounded and unreliable).  Deletion stays manual, and the recipe must
include the local side (`git remote remove github` in `zarr_root/<id>`, or
drop `refs/remotes/github/*`), otherwise G2.

`update-github-metadata` is left unchanged; it inherits the F5 fix and the
gate.

### C8 — (deferred) isolate Zarr failures inside a Dandiset sync

Catch per-Zarr exceptions in the nursery task, let siblings finish, raise a
summary — as `pool_amap()` does for Dandisets.  With C4 a cancellation no
longer *loses* Zarr-side state, but F8 still costs a manual cleanup per
failure, so this remains valuable; it changes stable semantics in
`asyncer.py` and is a follow-up.

### Alternative considered: the 10-line fix

One global `anyio.Lock` around `create_sibling_github` with 1 s spacing would
very likely have prevented *this* incident (≤ 60 creations/min < 80) and is
the core of PR1.  It is not enough on its own: each new Zarr is a POST plus a
PATCH, so continuous onboarding still reaches 500/h after ~8 min (~250
Zarrs/h); it leaves F1 (no retry), F4 (nothing pushes/describes later), F7/F8
(dirt) and `some default` (unless `description=` is passed) untouched, and
fixes nothing after the fact.

## 7. Rollout

### Step 0 — triage on the production host (no code)

1. From the failed run's log: `grep 'Job failed on input'` → the failed
   Dandisets.  In each: `git status --porcelain`; if only
   `.dandi/assets.json`, `dandiset.yaml` and staged blobs, reset by hand
   (`git reset --hard`, plus `git clean -dfx` if untracked files remain;
   annex objects and the git-annex branch survive; everything is re-derived
   from the archive).  Manual by decision; the `Dirty …` error in the run
   log now carries the first 10 dirty paths (C5).
2. Under `zarr_root` and any `backup-zarrs` partial dir: list Zarrs that are
   not installed (S1), dirty (S3/S5), or have a `github` remote with no
   tracking ref / unpushed commits (S6) / no `dandi.github-description`
   (S7).  Put the counts in the PR — they size the first-run backlog
   (C4) and the `--limit` for `reconcile-zarrs`.
3. Confirm from the log that the DataLad credential and `GITHUB_TOKEN` are the
   same account (or note that they are not).

### Step 1 — PR1 (tiny; zero delta for runs that create nothing)

Gate lock + 1 s spacing around repo creation, `description=` +
`result_renderer="disabled"` records, `x-github-request-id` in error logging.
(Shipped together with step 2 in the end.)

### Step 2 — PR2 (the plan proper)

C1, C2 (cooldown / give-up), C3 (retry, both exception shapes,
`fail_after`, idempotent config), C4 (unpushed-commits predicate,
describe-when-missing, F5, S2), C5, C6, tests.

### Step 3 — PR3 (`reconcile-zarrs`), then run it

`reconcile-zarrs` report → review → `--fix --limit …`, repeated until clean.
Then the usual cron; a run that hits the content-creation limit now backs off
collectively, resumes, and if it gives up fails only the Zarrs it
could not create (and their Dandisets, which then need the manual reset of
step 0) — which the next run picks up.

### 7.1 Behavioural deltas for runs that never hit a limit

* Mutations (creation, PATCH, release) are serialised with ≥ 1 s spacing
  across all Dandisets in the process — a slow DataLad creation (≤ 120 s)
  delays other Dandisets' creations, PATCHes and releases; new repos get a
  real description (`Zarr <id> of Dandiset <n>` / `Dandiset <n>`); DataLad's
  result lines disappear from stdout and reappear as our DEBUG log lines,
  while DataLad still emits one ERROR line per failed (then retried) attempt.
* C1: 429 retried for GitHub where today fatal; a rate-limit-shaped 403 on
  `get_repo`/`edit_repo`/`create_release` now sleeps for what GitHub says
  (else ≥ 60 s) instead of 1 s; once the gate has given up, reads fall back
  to the pre-PR bounded retry and mutations raise `GitHubRateLimited`;
  DANDI/S3 unchanged.
* C4: a handful of local git reads per Zarr visit (`rev-parse`, `config
  --get`, `merge-base`) and 3 `config --get` on the existing-sibling path; on
  the first run after deploy, a push for every Zarr with unpushed commits and
  a PATCH + `get_stats` walk for every cache-less Zarr (paced by the gate;
  population from step 0); `set_zarr_description()` signature; `backup-zarrs`
  with GitHub stops crashing on fresh Zarrs (F5); dirty-dataset errors carry
  a digest.
* Unembargo (`update_zarr_repos_privacy`) and `update-github-metadata` PATCH
  bursts are now paced at ~1/s.

### 7.2 Observability (so the next incident is diagnosable from the duct log)

What the duct log now carries:

* One `RATELIMIT` WARNING per classified response: operation and URL, the
  headers `_describe_http_error()` collects (`retry-after`,
  `x-ratelimit-{limit,remaining,used,reset,resource}`,
  `x-github-request-id` — needed for GitHub support tickets), the body
  excerpt, the consecutive-hit count against the give-up threshold, the
  chosen sleep and its source (`retry-after` / `x-ratelimit-reset` /
  `fallback`).  Hits reported by DataLad's error record carry the message
  text only (no headers).
* One `GAVE-UP` WARNING when the gate stops retrying; `GitHubRateLimited` is
  the dedicated exception class, so the `Job failed on input …` traceback
  says why a Zarr/Dandiset failed.
* Dirty-dataset errors carry the `describe_dirt()` digest.

Follow-ups, not in this PR: `COOLDOWN start/end` lines, `/rate_limit`
samples, per-Zarr outcome lines, an end-of-run summary, logging in
`_set_github_description()`, startup logging of the constants.

### 7.3 What a rate-limited run costs after this plan

Zarr side: nothing sticky for the rate-limited Zarr itself (C3/C4); Zarrs
cancelled mid-sync (S3/S5) stay dirty and are reported (C5).  Dandiset side:
a Zarr that fails after the gate gives up still fails its Dandiset, which
is then dirty (F8), reported, and reset by hand.  That is why a few bounded
retries are worth spending rather than failing fast: every failure still
costs a manual cleanup.

### 7.4 Cron

The invocation is already wrapped in an overall `flock` (maintainer), so
runs cannot overlap; the code base itself has no lock (`__main__.py`,
`datasetter.py`), which is fine.  The C2 give-up therefore bounds the run's wall
time so that a sleeping run does not starve the next scheduled one (with
`flock -n` the next tick is skipped, with a blocking `flock` it is delayed);
they are not there to prevent overlap.

## 8. Tests (`@pytest.mark.ai_generated` for AI-written ones; register the marker in `pyproject.toml` — it is not today)

`@pytest.mark.ai_generated` on AI-written tests; the marker is registered in
`tox.ini` (`[pytest]` lives there, not in `pyproject.toml`).  Existing tests
were tuned in preference to new ones.

Done in this PR:

* `test/test_aioutil.py`: the three original tests kept verbatim as the
  no-gate regression, with the local HTTP server generalised to per-response
  headers and non-GET methods; `is_rate_limited` classifier; `arequest` with
  a gate — 429 + `Retry-After` retried after exactly that wait, 403 with
  `x-ratelimit-remaining: 0` slept until `x-ratelimit-reset` (derived from
  the injected clock), 429 without a gate still fatal, a permanently
  rate-limited GET stays bounded once the gate gives up (`Retry-After: 0`
  clamped to 1 s), a mutation raises `GitHubRateLimited` on the give-up
  response; gate unit tests with an injected clock — fallback escalation,
  a hit inside a running cooldown does not escalate, reset on success,
  header precedence, give-up on the (N+1)-th hit, spacing, reads still sleep
  out the cooldown after give-up.
* `test/test_zarrbargo.py::test_sync_zarr_with_embargo_status`: `manager.gh`
  with a gate, `has_unpushed_commits` mocked, the extended
  `create_github_sibling` call signature, `ZarrSyncer.run` patched.
* `test/test_zarr.py` (docker-free, real `datalad create` datasets and a bare
  `github` remote): `create_github_sibling` — rate-limit error record then
  success (one 60 s sleep, description passed through, config set), 429
  `requests.HTTPError` honouring `Retry-After`, a non-rate-limit error
  propagates without retry, give-up after N slept-out hits, restore of wiped
  `pushurl`/`branch.*` without calling DataLad; `has_unpushed_commits`
  (no upstream → warning + False; never pushed; pushed; new commit);
  `sync_zarr` convergence — first visit pushes (plain) and PATCHes once,
  converged visit does neither, deleted tracking ref → push, missing cache →
  one PATCH, dirty → raises with the digest; F5 regression (cache lands in
  the given dataset, never in the global config; id-based form refuses a
  missing dataset).  The convergence test relies on DataLad's `Dataset`
  flyweight (the test's dataset object shares the `ConfigManager` with the
  one inside `sync_zarr`) and forces a config reload after its out-of-band
  `git config --unset`.

Follow-ups: the `move_on_after` timeout path, `has_unpushed_commits` with a
remote that is ahead / diverged, `format_result_message` arity fallback,
`describe_dirt` truncation, `_create_sibling_once` against real multi-record
DataLad output, an end-to-end docker run with a fake creation that fails one
Zarr past the give-up, `pool_amap` isolation.

## 9. Open questions for maintainers

Decided: dirty datasets remain a hard error, no automated stash / reset /
clean — reporting only (C5); F8 likewise; an overall `flock` already wraps
the invocation, so no cron change; no locally invented quota numbers —
sleeps come from GitHub's headers or its documented fallback, no per-run
mutation budget; pushes are plain, `--force` only under `--force-push`;
Dandisets get a real `description=` (`Dandiset <n>`) at creation too.

1. **Dandiset-side unpushed-commits push.**  `update_dandiset()` is untouched
   on this branch; apply the C4 predicate there too?
2. **Move creation after commit (C3).**  Left out: it reorders `sync_zarr`
   but would remove the rate-limited-Zarr-is-dirty case entirely.
3. **Constants.**  Four in `consts.py`: `GITHUB_MUTATION_SPACING` (1 s) and
   `GITHUB_RATE_LIMIT_FALLBACK` (60 s) are GitHub's own numbers;
   `GITHUB_RATE_LIMIT_ATTEMPTS` (5 slept-out hits) and
   `GITHUB_CREATE_TIMEOUT` (120 s) are ours.
4. **Orphan repos** (C7, follow-up): report only, or also `--delete-orphans`
   behind a confirmation?

## 10. Explicitly out of scope

* Reimplementing repo creation outside DataLad.
* A limiter on GET traffic / all GitHub requests.
* Switching from a PAT to a GitHub App installation token.
* Automated recovery of dirty datasets (stash / `git reset --hard` /
  `git clean -dfx`) — separate feature; this branch only reports them.
* Changes to asset download / annex code paths; `asyncer.py` nursery
  semantics (C8, follow-up).
* Dandiset-side flow (`update_dandiset`), except the shared
  `create_github_sibling()` / `edit_repo` improvements it inherits.

## 11. Side findings (not rate-limit related; separate fixes)

* **`--force-push` has never force-pushed** (not fixed here).
  `AsyncDataset.push()` passes a bool but DataLad computes
  `force_git_push = force in ('all', 'gitpush')` (`push.py:425`) and does not
  validate the argument, so `force=True` is a plain push.  Fix:
  `force="gitpush"`.  No test covers it; `CLAUDE.md` documents the feature.
* The comment at `zarr.py` claiming the description gate avoids a
  `GET /repos` per Zarr — rewritten in this PR.
* `AsyncDataset.push()` only retries `CommandError`; a rejected ref surfaces
  as `IncompleteResultsError` and fails the task (unchanged).
* `pytest.mark.ai_generated` was used but not registered — registered in
  `tox.ini` in this PR.

## 12. Review log (v1 → v2)

Fact-check: F4 corrected (FORCE never pushes); F5 corrected (crash, not a
shared cache; stranded in `partial_dir`); F1/F6 reconciled; F9 added;
DataLad's 429/`HTTPError` path, tuple messages, separate credential, and the
per-account block added; missed GitHub paths and the 3-4 round trips per push
listed; `arequest` scope (DANDI/S3) and existing jitter noted; C4's tracking
ref premise verified with its caveats.
Design: C1 made opt-in; C2 rewritten as one incident state with a single
escalator, breaker, spacing for all mutations, and a per-run budget; C3
classifies both shapes, holds the lock across the loop, uses `fail_after` +
`abandon_on_cancel`, seeds the cache, moves creation after commit; C4 uses
upstream config + ancestry; C6's tag push dropped; `/rate_limit` caveat fixed;
the 10-line alternative and the three-PR staging added.
Ops: F8 and the debris matrix (§3.5) added; C7 became `reconcile-zarrs` over
`zarr_root` (not `update-github-metadata`); G2 handling; caps shortened and
`flock` required; observability (§7.2) and concrete test wiring (§8) added;
no new config keys/flags for throttling.
Rejected/adjusted: "fail fast instead of sleeping" — kept a bounded budget
because F8 makes every failure cost a manual cleanup; "don't cache the interim
description" vs "seed it" — seeded, with C4 treating the interim string as
not-yet-described; a periodic `/rate_limit` timer — replaced by
start/event/end samples.
v3 (maintainer decisions): F7 recovery dropped from C4 — dirty datasets stay
a hard error, with a dirty-state digest in the error, a `DIRTY` line per
dataset in the run summary, and the same digest in `reconcile-zarrs` (C5);
F8 stays manual; §7.4 rewritten — an overall `flock` already exists, so the
caps only bound run duration; §9 Q1/Q5/Q7 closed.
v4 (maintainer feedback): §2.5 added — what GitHub actually tells us and
what that leaves for local constants; C2's cumulative-time breaker and
per-run mutation budget dropped in favour of relaying GitHub's headers /
documented fallback and a give-up after N consecutive rate-limited
responses; "push when behind" renamed and defined as a plain push of commits
the remote lacks, `--force` only under `--force-push`; creation-after-commit
made optional; §6 opened with the fix in one paragraph.
v5 (post-implementation review by two independent senior-developer reviews):
doc trimmed to the shipped cut (C2–C5, §3.5, §7, §7.1–7.2, §8, §9, §11);
give-up semantics stated as "N slept-out hits, give up on the next"; code
fixes from the review — a rate-limited read no longer loops forever once the
gate has given up (falls back to the bounded pre-PR retry) and a mutation
raises `GitHubRateLimited` on the give-up response, `Retry-After: 0` is
clamped to 1 s, a `GitHubRateLimited` from the describe step no longer fails
an already-pushed Zarr, the creation timeout is detected with
`move_on_after` rather than by catching a `TimeoutError` the thread might
have raised itself.
