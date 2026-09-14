# Plan: surviving GitHub secondary rate limits when creating Zarr repositories

Status: **draft v4 for review** (2026-09-14).  Comment inline on the PR, or
edit this file directly.  Nothing below is implemented yet.

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

| #   | State                                                         | Next plain run today                            | After this plan                         | Manual?                |
| --- | ------------------------------------------------------------- | ----------------------------------------------- | --------------------------------------- | ---------------------- |
| S1  | `datalad create` interrupted; no valid repo                   | `create` refuses the non-empty dir, every visit | same                                    | `rm -rf`               |
| S2  | created, `initremote` not run (F9)                            | `get_keys_missing_from` raises, every visit     | C4: remote setup idempotent             | until C4 [a]           |
| S3  | `.gitattributes` / embargo config written, commit cancelled   | dirty → F7 error, every visit                   | same error + digest + `DIRTY` line (C5) | reset by hand          |
| S4  | sibling configured, our `pushurl` / `branch.*` unset (F9)     | pushes over HTTPS without `pushurl`             | C4: sibling config idempotent           | —                      |
| S5  | `ZarrSyncer` cancelled → dirty worktree                       | F7 error, every visit                           | same, reported (C5)                     | reset by hand          |
| S6  | committed, push cancelled                                     | **never pushed** (F4) [b]                       | C4 pushes on the next visit [b]         | —                      |
| S7  | pushed, never described (`some default`)                      | never (F4)                                      | C4 or C7                                | —                      |
| S8  | Zarr complete, Dandiset failed → no submodule                 | healed by the clone path once D1 is fixed       | same                                    | fix D1 first           |
| D1  | Dandiset dirty (F8)                                           | fails every run                                 | same, reported (C5)                     | **yes**: §7 step 0     |
| G1  | GitHub repo with no local dataset                             | invisible                                       | C7 `--orphans` report                   | delete by hand         |
| G2  | repo deleted on GitHub; local tracking ref still matches HEAD | not re-created, not pushed                      | C7 checks `git ls-remote` [c]           | or drop the remote [c] |

[a] `git annex initremote` by hand until C4 lands.
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

A small `GitHubGate` object held by `GitHub` (its own module or `aioutil.py`,
so `adataset.py` can receive it without importing `manager`).  All state is
touched only from the event loop (never from inside a DataLad thread), so no
lock is needed for the fields.  It encodes no GitHub quota numbers; it relays
what GitHub says (§2.5):

* **Cooldown**: `note_rate_limited(headers)` sets `cooldown_until =
  max(cooldown_until, now + delay)` with `delay` = `retry-after` if present;
  else until `x-ratelimit-reset` if `x-ratelimit-remaining` is 0; else
  GitHub's documented fallback for an unannounced secondary limit: 60 s,
  doubling per consecutive hit.  A hit that arrives while a cooldown is
  still running does not escalate (ten workers failing in the same second
  are *one* incident).  `wait()` loops until `cooldown_until` has passed.
* **Give-up**: after `GITHUB_RATE_LIMIT_ATTEMPTS` (proposed 5) consecutive
  rate-limited responses in the process, every further mutation raises
  `GitHubRateLimited` immediately, so the run ends with N failed Zarrs
  instead of every worker sleeping in turn.  Any successful mutation resets
  the counter.  With the doubling fallback, 5 attempts bound the wait at
  ~31 min; a `retry-after` from GitHub is honoured as given.  GETs observe
  the cooldown but never the give-up.
* **Serialisation + spacing**: an `anyio.Lock` plus ≥ 1 s between the *end*
  of one mutation and the start of the next — GitHub's own guidance — for
  repo creation, `edit_repo` and `create_release` alike.  This is what turns
  a cooldown expiry into one prober instead of ten simultaneous retries.
  Lock order is always `zarr_limit` → gate; nothing acquires `zarr_limit`
  under the gate; `backup_zarrs`' `dslock` is taken only after `sync_zarr`
  returns → no deadlock.
* No new YAML keys or CLI flags: the attempt count and the spacing live in
  `consts.py`, overridable via environment like
  `BACKUPS2DATALAD_TEXT_SIZE_LIMIT`; effective values are logged at startup.

Known, accepted limitation: a Zarr sleeping in the cooldown holds its
`zarr_limit` slot (`zarr.py:512`) and its Dandiset worker, so during a
cooldown *all* Zarr work in the process stalls.  Restructuring that is out of
scope; the give-up bounds it.

### C3 — retry-wrap `create_github_sibling()` (keep DataLad) (`adataset.py`, `zarr.py`, `datasetter.py`)

* Call `create_sibling_github(..., description=<interim>,
  result_renderer="disabled", on_failure="ignore", return_type="list")` and
  inspect the records ourselves: no more interleaved stdout, and messages are
  routed through our logger with the `Dandiset X: Zarr Y` prefix.
* Classify **both** shapes: an `error` record whose formatted
  `message` (a `(fmt, arg)` tuple) mentions a rate limit / abuse detection /
  "blocked from content creation", **and** `requests.HTTPError` with status
  403/429 (this one carries `retry-after`; pass it to the gate).  Any other
  failure is re-raised unchanged.
* Retry loop: hold the gate's lock across the *whole* loop (one prober),
  each sleep computed by the gate; once the gate has given up, raise
  `GitHubRateLimited`.  Wrap the thread call in `anyio.fail_after(120)` with
  `abandon_on_cancel=True` so a hung `requests.post` (no timeout in DataLad)
  cannot pin the lock or block cancellation.
* Interim description `Zarr <id> of Dandiset <nnnnnn> (backup in progress)`,
  **and seed `dandi.github-description` with it right after creation**, so
  "cache missing" is exact from deployment on and C4 can treat "missing or
  interim" as "not yet described".  Whether Dandisets also get an interim
  string is §9 Q2.
* Make the post-creation local config idempotent (F9/S4): set
  `remote.github.pushurl`, `branch.draft.remote/merge` whenever they are
  missing, not only on the creation path.
* *Optional, not in the first cut (§9 Q3):* move the creation call from
  before the content sync (`zarr.py:543-565`) to just before the push (after commit/gc, `:599-605`), keeping the
  embargo-status commit where it is.  Then a rate-limited Zarr has already
  committed its content (never dirty, S3/S5 only arise for cancelled
  siblings) and a cooldown delays only the push/describe tail while content
  syncs continue.  §9 Q3.

### C4 — make the Zarr GitHub state converge on every visit (`zarr.py`, `manager.py`, `adataset.py`)

This is what fixes S2, S4, S6, S7 and lets a rate-limited run simply be
re-run for the Zarr side.

* **Push unpushed commits — a plain push, never forced.**  Replace the
  `made_commit` push gate with `made_commit or unpushed`, computed locally:
  resolve the
  upstream from `branch.<current>.remote/.merge` (set at `adataset.py:781-782`;
  do not hardcode `draft`); no upstream → warn, don't push; tracking ref
  absent → push; else push iff `git merge-base --is-ancestor HEAD <tracking>`
  is false, i.e. HEAD has commits the remote lacks.  If the remote has
  diverged, the plain push is rejected and the Zarr fails as it does today;
  `--force` is passed only under `--force-push`, exactly as now (and see
  §11: that flag currently does not force at all).  The
  git-annex branch is deliberately excluded (it is always ahead —
  `backup_zarrs` runs `annex describe here` after the push,
  `datasetter.py:599-601`).  Caveat G2: a stale tracking ref after a manual
  deletion on GitHub is C7's job.
* **Describe when not yet described.**  Gate: `made_commit or FORCE or cache
  missing or cache is the interim string`.  `_set_github_description()`
  already PATCHes only on a difference, so converged Zarrs cost zero API
  calls on no-op runs.  The Zarr path has no `GET /repos` (the Dandiset gate
  at `datasetter.py:247-259` exists for that GET; the comment at
  `zarr.py:620-623` claiming one is wrong).  First-run backlog: every
  `backup-zarrs` Zarr (F5), every Zarr from before `bb91ccd`, and every
  failed PATCH lacks a cache → one PATCH plus a `get_stats()` walk each,
  paced at 1/s by the gate — if GitHub objects, the cooldown / give-up
  applies and the next run continues; measure the population first (§7
  step 0).
* **Fix F5.**  `set_zarr_description()` takes the `AsyncDataset` actually on
  disk (the caller has it); keep the id-based form for
  `update_github_metadata()`; assert `ds.ds.is_installed()` so the
  global-config fallback can never happen silently again.
* **Idempotent remote setup (S2).**  `ensure_installed()` verifies the
  `dandiapi` and backup special remotes exist even when the dataset already
  is (cheap `git annex` config reads), instead of returning early.
* **Dirty datasets stay a hard error** (F7/F8, maintainer decision); the
  error and the run summary gain a digest instead — see C5.

Not touched: `datasetter.update_dandiset()` (§9 Q1).

### C5 — diagnostics: rate-limit report, identity check, dirty-dataset digest (`manager.py`, `datasetter.py`, `adataset.py`, `zarr.py`)

* `GitHub.get_rate_limit()` → `GET /rate_limit`, **best-effort** (log, never
  raise): at run start, on every rate-limit event, and at run end — not on a
  timer.  Logs `resources.core.remaining/limit/reset`.  Caveat (§2.4): it
  reports primary budgets only and can itself count against the secondary
  limit; it is diagnostics, not protection.
* At startup, log the account behind `GITHUB_TOKEN` (`GET /user`) and the
  account behind DataLad's `api.github.com` credential, and warn if they
  differ (F2 caveat).
* **Dirty-dataset digest.**  `AsyncDataset.is_dirty()` gets a sibling
  `describe_dirt()` that condenses `git status --porcelain` into: counts per
  status (`M`/`A`/`D`/`??`/…) grouped by area (Zarr content, `.dandi/`,
  `.datalad/`, other), the first ~10 paths, HEAD and its date.  The
  `is dirty; clean or save before running` errors at `zarr.py:566` and
  `datasetter.py:273` carry it; the per-Zarr outcome line reports
  `failed:dirty`; and the end-of-run summary lists every dirty dataset as
  one line — `DIRTY <Dandiset> [Zarr <id>] <digest>` — so `grep DIRTY` on the
  duct log yields the operator's reset list.  `reconcile-zarrs` (C7) shows
  the same digest per Zarr.  Read-only: no stash, reset or clean.

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
not REST)? unpushed commits (C4 predicate)? description cache missing/interim? dirty?
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
   from the archive).  Manual by decision; once C5 lands the run's `DIRTY`
   lines give this list directly.
2. Under `zarr_root` and any `backup-zarrs` partial dir: list Zarrs that are
   not installed (S1), dirty (S3/S5), or have a `github` remote with no
   tracking ref / unpushed commits (S6) / no `dandi.github-description` or an interim
   one (S7).  Put the counts in the PR — they size the first-run backlog
   (C4) and the `--limit` for `reconcile-zarrs`.
3. Confirm from the log that the DataLad credential and `GITHUB_TOKEN` are the
   same account (or note that they are not).

### Step 1 — PR1 (tiny; zero delta for runs that create nothing)

Gate lock + 1 s spacing around repo creation, `description=` +
`result_renderer="disabled"` records, seed the description cache at creation,
`x-github-request-id` in error logging.  ~20-30 lines.

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

* PR1: mutations (creation, PATCH, release) are serialised with ≥ 1 s
  spacing across all Dandisets in the process — a hung DataLad call now
  delays other Dandisets' creations (bounded by `fail_after`); new repos get
  the interim description (Dandisets too if §9 Q2 says yes); DataLad's
  result lines disappear from stdout and reappear as our log lines.
* C1: 429 retried for GitHub where today fatal; rate-limit-shaped 403s
  retried on `create_release`; a rate-limit 403 on `get_repo`/`edit_repo`
  now sleeps ≥ 60 s first instead of 1 s; DANDI/S3 unchanged.
* C4: one `git config` + `git merge-base` per Zarr visit; on the first run,
  a push for every Zarr matching the predicate and a PATCH + `get_stats`
  walk for every cache-less Zarr (paced by the gate; population from
  step 0); `set_zarr_description()` signature; `ensure_installed()` reads
  annex config on every visit; `backup-zarrs` with GitHub stops crashing on
  fresh Zarrs (F5); dirty-dataset errors carry a digest and the run summary a
  `DIRTY` line per dataset (C5).
* C5: two or three extra GETs per run; startup INFO lines.
* Unembargo (`update_zarr_repos_privacy`) and `update-github-metadata` PATCH
  bursts are now paced at ~1/s.

### 7.2 Observability (so the next incident is diagnosable from the duct log)

* One grep-able `RATELIMIT` WARNING per classified response: operation
  (create / PATCH / release), `org/repo`, status, `retry-after`,
  `x-ratelimit-{limit,remaining,used,reset,resource}`,
  `x-github-request-id`, body message, attempt i/N, chosen sleep,
  `cooldown_until` (UTC), cumulative cooldown.
* `COOLDOWN start/end` INFO with duration, waiter count, trigger; `BREAKER
  tripped` WARNING; `/rate_limit` samples (C5).
* A per-Zarr terminal INFO line with a fixed vocabulary
  (`outcome=created|pushed|described|up-to-date|failed:<class>`).
* End-of-run summary at WARNING when anything failed: counts per outcome,
  failed Zarr ids with Dandiset ids and failure class, rate-limit events,
  total cooldown, whether the gate gave up, and the exit reason
  ("N Dandisets failed, M due to GitHub rate limiting") — today the cause is
  buried in `Job failed on input …` tracebacks (`aioutil.py:221`).
  `GitHubRateLimited` is the dedicated exception class that makes this
  classification possible.
* `_set_github_description()` logs "unchanged; skipping" (DEBUG) /
  "PATCHing description" (INFO); throttle constants logged at startup.

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

* `arequest`: extend `test/test_aioutil.py`'s local HTTP server
  (`_make_handler`, `:38-67`, already emits `Retry-After`/`X-RateLimit-*`)
  with 429, a plain 403 (not retried unless in `retry_on`), a 403 with
  `x-ratelimit-remaining: 0`, and a regression asserting **unchanged**
  behaviour without a `rate_limiter` (the DANDI path).  Inject `clock`/`sleep`
  rather than monkeypatching `anyio.sleep`.
* Gate: pure unit tests with an injected clock — one incident from ten
  concurrent failures escalates once; cooldown deadline moving while
  sleeping; counter reset after success; after N consecutive hits later
  mutations fail fast; `retry-after` honoured over the fallback; 1 s
  spacing; no-op when nothing tripped.
* `create_github_sibling()`: real `AsyncDataset` in `tmp_path`;
  monkeypatch `ds.ds.create_sibling_github` to return an error record with a
  tuple message once, then a fake that does `git remote add github <bare>`;
  a `requests.HTTPError` 429 with `Retry-After`; a non-rate-limit error
  propagates immediately; lock released when the thread hangs
  (`fail_after` + `abandon_on_cancel`); idempotent config on a second call.
* `sync_zarr()` convergence (docker-free): patch `ZarrSyncer.run` to an
  `AsyncMock`, give the Zarr a local bare `github` remote (real `datalad
  push` → real tracking ref), `manager.gh = MagicMock(edit_repo=AsyncMock())`
  as in `test_zarrbargo.py:27-43`.  First visit pushes; tracking ref deleted
  → no-change visit pushes; remote ahead → no push; diverged → plain push
  rejected, no `--force`; no upstream config → no
  push + warning; converged → no push and no `edit_repo`; cache
  missing/interim → exactly one PATCH; a dirty Zarr still raises, and the
  message carries the digest (areas, counts, first paths).
* `describe_dirt()`: unit test on a fixture repo with modified, added,
  deleted and untracked paths across content / `.dandi/` / `.datalad/`;
  end-of-run summary emits one `DIRTY` line per dirty dataset.
* F5 regression: dataset under `tmp_path/partial/<id>`, assert the cache
  lands in that dataset's local config and nothing in the global one
  (`tmp_home`, `conftest.py:82-105`); the id-based form raises on a
  non-installed path.
* End-to-end on the docker `new_dandiset` fixture with two Zarrs and a fake
  creation that keeps failing one Zarr until the gate gives up: the other
  Zarr converges, a
  second run converges the failed one, and the Dandiset's failure is
  reported with a `DIRTY` line.  `pool_amap` isolation: Dandiset B completes
  while A is rate-limited.
* `reconcile-zarrs`: `--orphans` against the local HTTP server with `Link`
  pagination; predicate reporting on a fixture tree covering S2/S4/S6/S7.

## 9. Open questions for maintainers

Decided (v3): dirty datasets remain a hard error, no automated stash / reset /
clean on this branch — reporting only (C5); F8 likewise; an overall `flock`
already wraps the invocation, so no cron change is needed.
Decided (v4): no locally invented quota numbers — sleeps come from GitHub's
headers or its documented fallback, no per-run mutation budget; pushes are
plain, `--force` only under `--force-push`.

1. **Dandiset-side unpushed-commits push.**  Keep `update_dandiset()` untouched on
   this branch (proposed) or apply the C4 predicate there too?
2. **Interim description for Dandisets.**  `ensure_github_remote()` is the
   same function; passing `description=` there costs nothing, but for a fresh
   Dandiset with no changes the interim string persists until the next
   change.  Pass it (proposed) or leave Dandisets on `some default`?
3. **Move creation after commit (C3).**  Optional, left out of the first
   cut unless wanted: it reorders `sync_zarr` but removes the
   rate-limited-Zarr-is-dirty case entirely.
4. **Constants.**  Only two remain ours: the give-up count (proposed 5
   consecutive rate-limited responses) and `fail_after(120)` for DataLad's
   untimed POST.  Everything else is relayed from GitHub's headers or its
   documented fallback (§2.5).
5. **Orphan repos** (C7): report only, or also `--delete-orphans` behind a
   confirmation?

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

* **`--force-push` has never force-pushed.**  `AsyncDataset.push()` passes a
  bool (`adataset.py:492`) but DataLad computes
  `force_git_push = force in ('all', 'gitpush')` (`push.py:425`) and does not
  validate the argument, so `force=True` is a plain push.  Fix:
  `force="gitpush"`.  No test covers it; `CLAUDE.md` documents the feature.
* The comment at `zarr.py:620-623` says the description gate avoids a
  `GET /repos` per Zarr; the Zarr path has no such GET.
* `AsyncDataset.push()` only retries `CommandError`; a rejected ref surfaces
  as `IncompleteResultsError` and fails the task.
* `pytest.mark.ai_generated` is used but not registered.

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
