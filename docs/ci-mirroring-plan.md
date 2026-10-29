# Plan: running the mirror update in CI

Status: **v3 -- prototype** (2026-10-29).  v1 and v2 were reviewed by the
maintainer; §12 lists what changed since.  Line references are against `main`
at `3a0cc4d`.

Goal: move the `backups2datalad-update-cron` invocation (every 15 min on
drogon) into CI.  Nothing in the design depends on 15 minutes: that is only
how often a run is *started* -- a run may take longer (overlaps are
serialised, §5.1) -- and the schedule is the interim trigger until the DANDI
archive notifies us of changes itself (§5.5).  Target: GitHub Actions on
`dandi/dandisets` first, later Forgejo Actions on `forge.dandiarchive.org`
(forgejo+aneksjo, likely on falkor, with workers on typhon/drogon).  Each run
starts from a fresh checkout of the superdataset, installs **only** the
Dandisets (and Zarrs) that need work, processes them in parallel, and shows
that activity as CI jobs.

The workflows stay thin: every job checks out the superdataset, installs
backups2datalad, and runs one subcommand.  What to sync, how, and what to
record is backups2datalad's business, so drogon and CI run the same logic.

## 1. What runs today

`dandisets/tools/backups2datalad-update-cron` (via `-cron-common`):

1. activates a conda env and `pip install`s backups2datalad from `master`;
2. sources `.git/secrets` (DANDI API key, GitHub token);
3. `backups2datalad -l WARNING --backup-root <parent of superds> --config
   tools/backups2datalad.cfg.yaml update-from-backup --workers 5
   -e '00(0571|1412|1411)$'`;
4. `git pull --commit --no-edit` on the superdataset, then `datalad push -J 5`
   (the superdataset only -- subdatasets were pushed inside step 3).

The config puts Dandiset mirrors under `/mnt/backup/dandi/dandisets` (the
superdataset, **every** subdataset installed) and Zarr mirrors, flat and
outside any superdataset, under `/mnt/backup/dandi/dandizarrs`.  Nothing
prevents two runs from overlapping.

## 2. Where the code assumes a full local clone

### 2.1 Hazards (would corrupt or duplicate mirrors)

* **H1/H2 -- an uninstalled Dandiset or Zarr is silently re-created.**  Fixed
  in dandi/backups2datalad#124 (merged): `ensure_installed()`
  (`adataset.py:107`) takes a `before_create` hook, and creation is refused
  (`MirrorMissingError`) if `.gitmodules` (of the superdataset, or of the
  Dandiset for a Zarr) or GitHub already has the mirror.  `repo_exists()`
  trusts a 404 only from a token that can see private repositories, so the
  CI token must cover private repos in both `dandisets` and `dandizarrs`.
* **H3 -- a fresh git-annex repo per run.**  Every CI clone that runs
  `git annex init` records a new UUID in the `git-annex` branch, which is
  then pushed.  The mirrors never hold annexed content (blobs are `fromkey
  --force` + `registerurl`; text ≤ 10 MiB goes to Git; Zarr entries too), so
  a CI clone must have `annex.private=true` **before** git-annex first
  touches it.  `datalad clone` initializes on its own, so the prototype
  clones with plain `git clone`, sets `annex.private`, then `git annex init`
  (`AsyncDataset.clone_mirror()`); the workflow also sets it globally.
* **H4 -- public logs of embargoed Dandisets.**  `dandi/dandisets` is
  public, so its Actions logs and artifacts are world-readable.  See §6.

### 2.2 Per-run work that needs every mirror present

For **every** Dandiset in the API listing, every run:

* `get_backup_state()` -- local reads of `.dandi/assets-state.json`.
* `tag_releases()` (`datasetter.py:515`) -- one paginated `/versions/` API
  call **plus** a `git tag -l` per published version, even when nothing was
  published: ~1400 API calls per run.  §3.2 shows why `plan` can skip it.
* `get_stats()` (`datasetter.py:294`) -- the `dandi.stats` cache lives in
  each subdataset's `.git/config` (`adataset.py:1313`); a recount needs every
  Zarr of the Dandiset in `zarr_root` (`get_zarr_sub_stats()`,
  `adataset.py:1287`), changed or not.

And after the loop, `set_superds_description()` (`datasetter.py:428`) walks
**all** subdatasets, fails if any is not installed, and recounts any whose
cache is stale -- on every run that pushes to GitHub, explicit IDs or not.
The "deleted Dandiset → make private" pass reads only `.gitmodules`.

### 2.3 State that lives only on drogon

| State                          | Where today                               | Where it goes                                       |
| ------------------------------ | ----------------------------------------- | --------------------------------------------------- |
| Dandiset stats                 | `dandi.stats` in each `.git/config`       | `.gitmodules` of the superdataset (§3.1)            |
| Zarr stats                     | `dandi.stats` in each Zarr's `.git/config`| open (§4.8)                                         |
| last GitHub description        | `dandi.github-description`, `.git/config` | nowhere -- compare with GitHub's own (§3.3)         |
| per-run debug log              | superds `.git/dandi/backups2datalad/`     | job logs                                            |
| Zarr mirrors                   | `/mnt/backup/dandi/dandizarrs/<id>`       | cloned from `dandizarrs/<id>` on demand             |
| GitHub credentials             | `~/.ssh` + `hub.oauthtoken`               | token + `url.<https+token>.insteadOf` (§4.7)        |
| annex repo identity            | one UUID per mirror on drogon             | `annex.private=true` (H3)                           |

Other tooling that assumes the full tree (`tools/git-annex-info`,
`generate_whereis`, `du_dandisets`, `derivatives/`) is not part of the
scheduled update and stays on drogon.

## 3. Per-Dandiset state in `.gitmodules`

Per-submodule facts belong in `.gitmodules`, next to `url`, `datalad-id` and
`github-access-status`; the commit is already the gitlink.  No separate
index file.

### 3.1 Keys (prototyped)

What dandi/backups2datalad#69 proposed, finished and extended
(`mirrorstate.py`):

| key                          | value                                       | used for                              |
| ---------------------------- | ------------------------------------------- | ------------------------------------- |
| `dandiset-last-modified`     | committed `assets-state.json` timestamp     | `plan`: is the mirror stale?          |
| `dandiset-tagged-releases`   | sorted, comma-separated tags                | `plan`: anything new to tag?          |
| `dandiset-stats`             | `<commit>,<files>,<bytes>` incl. Zarrs      | description, seeding `dandi.stats`    |
| `backup-runner` *(later)*    | runner label, e.g. `drogon`                 | routing Dandisets CI cannot finish    |

* **Embargoed Dandisets get no `dandiset-stats`.**  `.gitmodules` is public;
  their gitlinks already show *that* they changed, but sizes and file counts
  would be news.  The superdataset description therefore leaves them out of
  the total when it works from `.gitmodules` (it still counts them).
* `<commit>` must equal the gitlink; anything else means "stale".
* Written by `update-from-backup` (after its superdataset commit) or by
  `record`, only where a value differs -- churn is proportional to the work
  done.  The first drogon run with this version bootstraps the keys for every
  mirror it visits, which is all of them; nothing else is needed.  Until
  then, `plan` sees `no-state` for every Dandiset.
* #69's problems are avoided: only real changes get gitlinks (`changed`),
  `last_modified` is the state in `HEAD` (`get_committed_assets_state()`), not
  the working tree, and the keys go in their own commit so existing commit
  counts in the tests hold.

### 3.2 Is dropping the per-Dandiset `/versions/` call safe?

`tag_releases()` has queried every Dandiset's versions on every run since it
was introduced (`8747c2d`, 2021-07-07); the async rewrite (`1f8ec72`, 2022-06)
kept it.  There was no stated reason beyond discovering new publications.

Evidence from the superdataset history (2024-06 → 2026-09) that publishing
bumps the draft's `modified`: in all 378 superdataset commits that tag a new
release, the same Dandiset also got a sync commit in the same run, which
only happens when the draft's `modified` moved.  Unembargo likewise: each of
the ~20 cron-made private → public flips of `github-access-status` checked
comes with that Dandiset's "Update embargo status" commit in the same run.

So `plan` decides on `modified`, and checks the listing's
`most_recent_published_version` against `dandiset-tagged-releases` and the
listing's embargo status against `github-access-status` as free extra checks
(no API calls).

### 3.3 `dandi.github-description`

A memo of the last description sent (`067f62c`, 2022-09), to skip a `PATCH`.
`set_dandiset_description()` already `GET`s the repository, so it can
compare with the returned `description`/`homepage` and drop the cache for
Dandisets.  For Zarrs, `sync_zarr()` sets a description whenever the cache
is missing -- in CI every Zarr it syncs; with `asset_checksum` mode only
changed ones.  Acceptable; not changed by the prototype.

## 4. Changes to backups2datalad

Each is useful on drogon as well.  Status in brackets.

1. **Guard against re-creation** [merged, #124].
2. **`.gitmodules` state** (§3) [prototyped].
3. **`backups2datalad plan`** [prototyped]: needs only the superdataset
   checkout and one `/dandisets/?embargoed=true` listing.  A Dandiset is
   planned if it is new, has no recorded state, was modified since, has an
   untagged release, or its embargo status differs from
   `github-access-status`; `--exclude`, the quiescent period and
   `UNEMBARGOING` skip it, as in `update_dandiset()`.  `--embargoed
   skip|include|only` (default `skip`: counted, never named), `--max N`
   (§5.3), `--mode force` (everything), `--format json|ids`.  It also lists
   public mirrors no longer on the archive.  Not yet wired into
   `update-from-backup` on drogon, where it would replace the ~1400
   `/versions/` calls.
4. **Install on demand** [prototyped]: `update-from-backup
   --clone-missing` clones a Dandiset mirror the superdataset records but
   that is absent (from its `.gitmodules` URL, remote named `github`, H3),
   and seeds its `dandi.stats` from `dandiset-stats` when that is for the
   cloned commit, so an unchanged mirror is never recounted.  The same for a
   Zarr whose sync is due (`Downloader.provide_zarr_mirror()`, where #124's
   guard sits) -- with `zarr_mode=asset_checksum` an unchanged Zarr is not
   cloned for syncing.  But see §4.8.
5. **Split "sync" from "record"** [prototyped]: `update-from-backup
   --results-dir DIR` leaves the superdataset alone (no commit, no
   deleted-Dandiset pass, no description) and writes `DIR/<id>.json` per
   Dandiset visited: the new commit, the state keys, the embargo status, the
   commit subjects since the recorded gitlink, and URL and DataLad ID for a
   Dandiset new to the superdataset.  `record RESULTS...` applies them to a
   superdataset checkout *without* subdatasets (`git update-index` of the
   gitlinks, `.gitmodules` keys), runs the make-private pass for deleted
   Dandisets (one archive listing), commits with the usual "N updated (…)"
   message, and pushes; if the push is rejected, it fetches, resets to the
   remote branch and records again, up to `RECORD_PUSH_ATTEMPTS` (5) times.
   Then it sets the superdataset's description from `.gitmodules`
   (`set_superds_description()` now falls back to the keys for mirrors that
   are not installed).  The make-private pass is GitHub-side and done once,
   before the retries; its `.gitmodules` edits are re-applied with the rest.
6. **Graceful time limit** [next] (`--time-limit SECONDS`, §5.4): stop
   taking new assets once the deadline passes, commit what is done, push,
   exit 0 with the Dandiset reported as "partial".  The listing is now
   fetched whole before any work (`fetch_stable_assets()`,
   `asyncer.py:723`), so the deadline can be checked per asset; the state
   written at the end (`asyncer.py:689`, `:709`) must then be the last
   processed asset's `created`, not the draft's `modified`, so the next run
   resumes.  A single huge Zarr is not split this way (`ZarrSyncer` commits
   once).
7. **Credentials** [workflow]: `.gitmodules` holds both forms -- most URLs
   are `https://github.com/...`, 64 are `git@github.com:...` (those of
   private mirrors).  Keep them, and in CI set
   `url."https://x-access-token:${TOKEN}@github.com/".insteadOf` for both
   `https://github.com/` and `git@github.com:`.  The same token serves the
   API (`GITHUB_TOKEN`) and DataLad's `create_sibling_github`
   (`DATALAD_CREDENTIAL_GITHUB_TOKEN`).  The prototype uses one classic
   token with `repo` scope (a secret `MIRROR_GITHUB_TOKEN`), since one token
   has to reach three organizations (`dandi`, `dandisets`, `dandizarrs`) and
   `repo_exists()` needs to see private repos.  A GitHub App installation
   token is per organization, so it would mean three tokens and three
   `insteadOf` prefixes -- worth it later for scoping and rate limits.
8. **Zarr stats without the Zarrs** [open]: a changed Dandiset is recounted,
   and the recount needs every one of its Zarrs' stats, which live in each
   Zarr's own `.git/config`.  `--clone-missing` makes `get_zarr_sub_stats()`
   clone the Zarr for that, which for a Dandiset with many big Zarrs is far
   too much.  Fix: record each Zarr's `<commit>,<files>,<bytes>` in the
   *Dandiset's* `.gitmodules` (`zarr-stats`, next to the Zarr gitlink; the
   Dandiset may be private, so this does not leak), written when the Zarr is
   synced, and read by `get_stats()` when it matches the gitlink.  Until
   then, Zarr-heavy Dandisets (001412, …) stay on drogon.

## 5. The workflow

Prototype: `dandi/dandisets` `.github/workflows/mirror.yml` (branch
`claude/jolly-meitner-v8jo47`).

### 5.1 Shape

```yaml
on:
  workflow_dispatch: {inputs: ...}     # §5.2; schedule once the cron stops
concurrency: {group: mirror, cancel-in-progress: false}
jobs:
  plan:    # checkout superds (no submodules); `backups2datalad plan`;
           # matrix = planned ids; summary table in $GITHUB_STEP_SUMMARY
  sync:    # one job per Dandiset: `update-from-backup --clone-missing
           # --results-dir results <id>`; upload results/ as an artifact
    strategy: {fail-fast: false, max-parallel: 5, matrix: {id: ...}}
  record:  # checkout superds; download the results; `backups2datalad record`
    needs: [plan, sync]
    if: always() && <anything planned>
```

Each Dandiset is its own job with its own log, status and duration -- the
per-Dandiset timing record we lack today comes for free.  A failing Dandiset
fails its job without blocking the others; `record` records whatever
succeeded and the run goes red.

### 5.2 Manual runs

`workflow_dispatch` inputs mirroring `update-from-backup`, passed to the
shell through the environment only (never `${{ }}` inside `run:`):

| input        | → option                          | notes                                         |
| ------------ | --------------------------------- | --------------------------------------------- |
| `ids`        | positional; `plan --mode force`   | blank = plan from `.gitmodules`               |
| `mode`       | `--mode`                          | `timestamp` / `force` / `verify`              |
| `zarr_mode`  | `--zarr-mode`                     | default `asset_checksum` in CI                |
| `max`        | `plan --max`                      | §5.3                                          |
| `exclude`    | `-e`                              | default `00(0571\|1412\|1411)$`, as the cron  |
| `extra_args` | appended to `update-from-backup`  | `--gc-assets`, `--force`, `--zarr-dirty`, ... |

`--force-push` stays out (it overwrites history; and it is still a no-op,
see CLAUDE.md).  `extra_args` keeps us under the cap on inputs.

### 5.3 How much one run takes on

`max-parallel` caps only how many matrix jobs run *at once*; the run takes on
the whole matrix, queued behind that cap.  So `plan --max N` caps the total:
a run finishes its batch in bounded time and the next run picks up the rest,
which is still stale in `.gitmodules` and therefore still planned.  Order:
up to a quarter of the batch goes to the Dandisets stale longest (so big ones
cannot starve), the rest smallest first by the draft's `size` from the
listing (so small updates are not stuck behind big ones).

No sharding: with a cap there is never more than `N` jobs, and the per-job
overhead (checkout, `pip install`, clone) is about a minute.  A mass re-sync
(`--mode force` over everything) is better done on drogon than as 1400 jobs.

### 5.4 Long-running backups

We regularly have one backup that runs for hours (001873 has been updating
on drogon for 12+ hours with git-annex mostly idle; #149's removal of a
`git rm` per new file, which cost 000571 15.5 h, may be the cause --
re-measure before designing around it).  In CI, `timeout-minutes` kills the
job: safe (nothing pushed, the clone discarded) but wasted, and the Dandiset
would be planned again and killed again.  Three layers:

1. **Resumable progress** (`--time-limit`, §4.6): long Dandisets finish over
   a few runs.
2. **Route what CI cannot finish** to a self-hosted runner: `backup-runner
   = drogon` in `.gitmodules`, by hand at first.  `plan` leaves it out of
   the matrix and lists it separately; a `sync-one.yml` with
   `concurrency: {group: dandiset-<id>}` runs it on `[self-hosted, drogon]`
   for as long as it takes.
3. **Record asynchronously**: `sync-one.yml` runs `record` itself for its
   one result (the push retry covers concurrent `record`s).

Also budget for GitHub: a repository creation is now retried through 5xx,
timeouts and lookup 404s (#143/#145/#147, up to ~10 min), and the gate
sleeps out rate limits for up to ~5 h before giving up -- the job timeout
(340 min in the prototype) is the backstop for both.

### 5.5 Other differences from cron

* **Trigger.**  `schedule: '*/15 * * * *'` once the cron stops, until the
  archive can tell us what changed: a webhook from dandi-archive on Dandiset
  modification/publication/unembargo → `repository_dispatch` carrying the
  IDs, so a run starts within seconds and `plan` confirms against the API;
  the schedule then stays as a slow safety net.  `schedule` is best-effort
  (5-30 min late, dropped under load); dandi-compute dispatches from a site
  cron instead.
* **Throttling.**  The `GitHubGate` is per process, so five parallel jobs
  are five gates; `max-parallel` is the only global throttle.
* **Runners.**  Hosted `ubuntu-latest` for public repositories: 4 vCPU,
  16 GB RAM, ~14 GB free disk, 6 h per job.
* **git-annex** ≥ 10.20240430 via `pip install git-annex` (PyPI wheels of
  the standalone build), alongside backups2datalad.
* **Pin** backups2datalad to a release, bumped by PR (the prototype points
  at its branch).
* **Failure mail with a cooldown** (dandi-compute's `check-cooldown`: one
  per 24 h per workflow), so a stuck Dandiset does not mail every run.
* Things that do not matter on fresh clones: `--zarr-dirty` (a clone is
  never dirty), the dirty-mirror gate.

## 6. Embargoed Dandisets

They must not be synced by a workflow in a public repository.  In the
public workflow, `plan --embargoed skip` (the default) counts them and never
names them, in the log, the JSON or the summary.  Until there is a private
place for them, they stay with the cron on drogon (§10).

Proposal: a **private** repository (e.g. `dandi/dandisets-private`) holding
only the workflow -- not a clone of the superdataset -- whose `plan` runs
`--embargoed only` against a checkout of the public superdataset, whose
`sync` jobs push the private mirrors, and whose `record` pushes to the
public superdataset.  That already carries the private submodules' gitlinks
and `github-access-status = private`, and gets no stats for them (§3.1), so
nothing new is revealed.  Its commit messages carry commit subjects, as
today's do.

Minutes: hosted runners on private repositories draw from the
organization's quota; self-hosted ones cost nothing.  Embargoed Dandisets
are few and change rarely, so either fits; a self-hosted runner on
drogon/typhon registered to the private repo only also keeps their data off
shared infrastructure.

## 7. Self-hosted runners: what runs where

* **Target**: all per-Dandiset jobs work in scratch clones, wherever they
  run.  Hosted runners take the bulk; `[self-hosted, drogon]` takes the
  `backup-runner = drogon` ones (§5.4) and embargoed ones (§6).  The
  superdataset of record is the one on GitHub, updated by `record`;
  `/mnt/backup/dandi/dandisets` on drogon becomes a consumer that `git pull`s
  for the other tools in §2.3.
* Self-hosted runners only for `schedule`/`workflow_dispatch` on the default
  branch (no PRs from forks), with a hosted "is the runner online?"
  pre-check job so a dead runner fails fast (dandi-compute's
  `check-runner`).

## 8. What dandi-compute does (reviewed 2026-09-24)

`dandi-compute-runner` + `dandi-compute-core`, on MIT Engaging: a public
repo for logs and self-hosted (SLURM) runners for work; a login-node cron
that dispatches only when there is work; workflows that only submit and
return, with a `workflow_run`-triggered reconcile; state with the data
(`derivatives/jobs.tsv`), with jobs *claiming* work; timeouts ending as
`failed`, with logs uploaded; mail at most once per 24 h.  Adopted above:
state in the data plus reconciling (`record`, §4.5), per-item concurrency as
the claim (§5.4), graceful deadline (§4.6), runner pre-check and fork policy
(§7), notification cooldown (§5.5), optional site-side dispatch (§5.5).

## 9. Forgejo + aneksjo later

* Forgejo Actions reads the same YAML (`runs-on` labels differ; `uses:`
  resolves against `data.forgejo.org` mirrors).
* `Manager.gh` / `GitHub` in `manager.py` is the only forge API client; put
  a small `Forge` protocol in front of it with a Forgejo (Gitea API)
  implementation, and make the sibling name (`github`, hard-coded throughout)
  configurable.
* With aneksjo the forge can hold annex content, which would make the
  (currently dead) `populate` step meaningful again.

## 10. Order of work

1. ~~H1/H2 guard~~ → #124, merged.
2. `.gitmodules` state, `plan`, `--clone-missing`, `--results-dir` +
   `record` → this PR (prototype).  Deploy on drogon first: its next cron run
   bootstraps the keys, with nothing else changing for it.
3. Zarr stats in the Dandiset's `.gitmodules` (§4.8).
4. Try the workflow by `workflow_dispatch` on a handful of small public
   Dandisets, with those excluded from the cron (`-e`) meanwhile.
5. `--time-limit` (§4.6).
6. Hand public Dandisets to the schedule: the cron runs only `plan
   --embargoed only --format ids` (plus the Zarr-heavy ones); `plan` in
   `update-from-backup` replaces the `/versions/` calls there.
7. Private repo for embargoed Dandisets (§6), `backup-runner` routing
   (§5.4); retire the cron.
8. `Forge` abstraction for Forgejo (§9).

## 11. Open questions

* `annex.private`, or reuse drogon's UUID in CI (`annex.uuid` set before
  `init`) so `whereis` history stays meaningful?
* Is `backup-runner` set by hand only, or by `record` after N deadline hits
  without progress -- and what counts as progress for a single huge Zarr?
* `record` once per run (as now) or once per job?  Per run keeps the
  superdataset history as it is; per job would let a long job record without
  waiting for the rest of the batch.

## 12. Changes since v2

* Prototyped §3.1, §4.3-4.5 in backups2datalad and the workflow in
  `dandi/dandisets`.
* Embargo: no stats keys for embargoed Dandisets; `plan` names them only
  with `--embargoed include|only` (both found by review).
* Updated for what landed on `main` since v2: the superdataset description
  needs every mirror installed (now falls back to `.gitmodules`), Zarr stats
  need the Zarr mirrors (§4.8, open), whole-listing fetch (§4.6), GitHub
  creation retries (§5.4), #149 and 001873 (§5.4); line references.
* §4.7: both URL forms are in `.gitmodules`; one token for three orgs.
* §5.3: a cap, no sharding (v2 had both).
* §2.1: `annex.private` has to precede the first git-annex call, which
  `datalad clone` makes.
* §5.5: current hosted runner specs; git-annex from PyPI.
