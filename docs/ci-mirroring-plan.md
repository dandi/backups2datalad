# Plan: running the mirror update in CI

Status: **v2 -- draft for discussion** (2026-09-24).  v1 was reviewed inline
by the maintainer; §10 lists what changed.  Line references are against
`main` at `90aacaf`.

Goal: move the `backups2datalad-update-cron` invocation (every 15 min on
drogon) into CI.  Nothing in the design depends on 15 minutes: that is only
how often a run is *started* -- a run may take longer (overlaps are
serialised, §5.1) -- and the schedule is the interim trigger until the DANDI
archive notifies us of changes itself (§5.5).  Target: CI -- GitHub Actions on `dandi/dandisets` first, later Forgejo
Actions on `forge.dandiarchive.org` (forgejo+aneksjo, likely on falkor, with
workers on typhon/drogon).  Each run starts from a fresh checkout of the
superdataset, installs **only** the Dandisets (and Zarrs) that need work,
processes them in parallel, and shows that activity as CI jobs.

## 1. What runs today

`dandisets/tools/backups2datalad-update-cron` (via `-cron-common`):

1. activates a conda env and `pip install`s backups2datalad from `master`;
2. sources `.git/secrets` (DANDI API key, GitHub token);
3. `backups2datalad --backup-root <parent of superds> --config
   tools/backups2datalad.cfg.yaml update-from-backup --workers 5
   -e '00(1412|1411|1449)$'`;
4. `git pull --commit --no-edit` on the superdataset, then `datalad push -J 5`
   (the superdataset only -- subdatasets were pushed inside step 3).

The config puts Dandiset mirrors under `/mnt/backup/dandi/dandisets` (the
superdataset, **every** subdataset installed) and Zarr mirrors, flat and
outside any superdataset, under `/mnt/backup/dandi/dandizarrs`.  The
`populate*` crons are dead (the Dropbox remotes are commented out of the
config), and the `-108` scripts are one-offs.  Nothing prevents two runs from
overlapping.

## 2. Where the code assumes a full local clone

### 2.1 Hazards (would corrupt or duplicate mirrors)

* **H1 -- an uninstalled Dandiset is silently re-created.**
  `update_dandiset()` always calls `init_dataset()` →
  `AsyncDataset.ensure_installed()` (`datasetter.py:220`, `adataset.py:85`),
  which runs `datalad create` whenever the directory is not an installed
  dataset: a new datalad-id and one-commit history under the existing name,
  and a push that collides with the real repository.  Also true on drogon
  for a mirror directory removed by hand.
* **H2 -- same for Zarrs**, via `sync_zarr()` (`zarr.py:522`) on
  `zarr_root/<zarr_id>`.
  → **Fixed in dandi/backups2datalad#124** (H1+H2): `ensure_installed()`
  gets a `before_create` hook, and creation is refused if `.gitmodules` (of
  the superdataset, or of the Dandiset for a Zarr) or GitHub already has
  the mirror.
* **H3 -- a fresh git-annex repo per run.**  Every CI clone that runs
  `git annex init` records a new UUID (plus `describe here`,
  `datasetter.py:626`) in the `git-annex` branch, which is then pushed.  The
  mirrors never hold annexed content (blobs are `fromkey --force` +
  `registerurl`, `annex.py:64`; text ≤ 10 MiB goes to Git; Zarr entries are
  `fromkey` + `registerurl` too), so CI clones set `annex.private=true`
  before `git annex init`.
* **H4 -- public logs of embargoed Dandisets.**  `dandi/dandisets` is
  public, so its Actions logs and artifacts are world-readable, and syncing an
  embargoed Dandiset logs asset paths, Zarr ids and commit messages.  See
  §6.

### 2.2 Per-run work that needs every mirror present

For **every** Dandiset in the API listing, every 15 minutes:

* `get_backup_state()` -- local reads of `.dandi/assets-state.json`.
* `tag_releases()` (`datasetter.py:256`, `:414`) -- one paginated
  `/versions/` API call **plus** a `git tag -l` per published version, even
  when nothing was published: ~1400 API calls per run, and the main reason
  the loop needs every clone.  See §3.2 for why it is safe to drop on no-op
  runs.
* `get_stats()` (`datasetter.py:261`) -- the `dandi.stats` cache lives in
  each subdataset's `.git/config` (`scope="local"`, `adataset.py:1014`);
  without it the stats are recounted, which for Zarr submodules needs
  `zarr_root/<id>` on disk (`get_zarr_sub_stats()`).
* `ensure_github_remote()` -- assumes a remote named `github`; a
  `datalad get -n` clone has `origin`.

After the loop, `set_superds_description()` (`datasetter.py:381`) sums
`get_stored_stats()` over **all** subdatasets' `.git/config` -- in CI it
would silently report 0 bytes.  The "deleted Dandiset → make private" pass
reads only `.gitmodules` and already works on a bare checkout.

### 2.3 State that lives only on drogon

| State                         | Where today                                 | Where it should go                                          |
| ----------------------------- | ------------------------------------------- | ----------------------------------------------------------- |
| Dandiset + Zarr stats         | `dandi.stats` in each `.git/config`         | `.gitmodules` of the superdataset (§3.1)                    |
| last GitHub description       | `dandi.github-description`, `.git/config`   | nowhere -- compare against GitHub's own (§3.3)              |
| `dandi.populated`             | `.git/config`                               | dead (no populate)                                          |
| per-run debug log             | superds `.git/dandi/backups2datalad/`       | job logs (+ artifact, private runs only)                    |
| Zarr mirrors                  | `/mnt/backup/dandi/dandizarrs/<id>`         | cloned from `dandizarrs/<id>` on demand                     |
| GitHub SSH key (private repos)| `~/.ssh` (`https_to_ssh_url`)               | `url.<https+token>.insteadOf git@github.com:`               |
| annex repo identity           | one UUID per mirror on drogon               | `annex.private=true` (H3)                                   |

Other tooling that assumes the full tree (`tools/git-annex-info`,
`generate_whereis`, `du_dandisets`, `derivatives/`) is not part of the
scheduled update and can stay on drogon.

## 3. Per-Dandiset state in `.gitmodules`

Per-submodule facts belong in `.gitmodules`, next to `url`, `datalad-id` and
`github-access-status`; the commit is already the gitlink.  No separate
index file.

### 3.1 Keys

Finishing dandi/backups2datalad#69 (draft since 2025-02, conflicts with
`main`), which proposed the first two:

| key                            | value                                              | used for                                   |
| ------------------------------ | -------------------------------------------------- | ------------------------------------------ |
| `dandiset-last-modified`       | committed `assets-state.json` timestamp            | `plan`: is the mirror stale?               |
| `dandiset-tagged-releases`     | sorted, comma-separated version ids                | `plan`: anything new to tag?               |
| `dandiset-stats`               | `<commit>,<files>,<bytes>` incl. Zarr content      | descriptions, superds total, ordering      |
| `dandiset-zarr-stats`          | `<zarrs>,<files>,<bytes>`                          | routing Zarr-heavy Dandisets (§5.4)        |
| `backup-runner` *(optional)*   | runner label, e.g. `drogon`; set by hand or §5.4   | routing Dandisets CI cannot finish         |

* `<commit>` in `dandiset-stats` must equal the gitlink; a mismatch means
  "stale, recount on next sync", as `get_stored_stats()` does today.
* Written only for Dandisets that changed in a run, so `.gitmodules` churn
  is proportional to the work done.
* Bootstrapped once on drogon from the existing clones (`dandi.stats`,
  committed `assets-state.json`, `git tag`).
* Known problems in #69 as it stands (for whoever finishes it): every
  Dandiset now counts as changed (`update_dandiset()` returns a report even
  when nothing was committed, so `to_save` holds all ~1400 paths every run);
  `last_modified` is read from the working tree rather than `HEAD`, which is
  what dandi/backups2datalad#123 had to stop trusting; and the tests pin
  commit indices that move with every extra `.gitmodules` commit.

### 3.2 Is dropping the per-Dandiset `/versions/` call safe?

`tag_releases()` has queried every Dandiset's versions on every run since it
was introduced (`8747c2d`, 2021-07-07, "Create tags for new releases"); the
async rewrite (`1f8ec72`, 2022-06) kept it as is.  There was no stated
reason beyond discovering new publications, and nothing else re-checks
existing tags.

Evidence from the superdataset history (2024-06 → now) that publishing
bumps the draft's `modified`: in all 378 superdataset commits that tag a new
release ("Merge '<version>' into drafts branch"), the same Dandiset also got
a sync commit in the same run -- which only happens when the draft's
`modified` moved past the recorded state.  Not once did a release get tagged
on its own.

Unembargo likewise: each of the ~20 cron-made private → public flips of
`github-access-status` checked (2025-09 → now) comes with that Dandiset's
"[backups2datalad] Update embargo status" commit in the same run, which runs
inside `sync_dataset()`, i.e. after a `modified` bump.  (The only flips
without one are in manual `Merge remote-tracking branch 'origin/draft'`
commits, not cron runs.)  While the Dandiset is `UNEMBARGOING` it is
skipped, and the bump arrives when that finishes.

So `plan` can decide on `modified` alone, and uses the listing's
`most_recent_published_version` against `dandiset-tagged-releases` as a
free extra check (no API call).  `describe_dandiset()` makes a second
`/versions/` call just to count releases; the key gives that too.

### 3.3 `dandi.github-description`

Added in `067f62c` (2022-09-22, "Set github description only if changed
from what was set to before") to skip a `PATCH` when nothing changed.  It is
a pure memo of the last value sent, derivable from stats + metadata.  And
`set_dandiset_description()` already `GET`s the repository to check its
visibility (`manager.py:68`), so it can compare against the returned
`description`/`homepage` instead and drop the cache for Dandisets.  For
Zarrs (no `GET` there today), `sync_zarr()` sets a description only when the
cache is missing -- in CI that is every Zarr it touches, which with
`asset_checksum` mode means only changed Zarrs; acceptable, or add the same
`GET` + compare.

## 4. Changes to backups2datalad

Each is useful on drogon as well, so they can land before any CI exists.

1. **Guard against re-creation** -- dandi/backups2datalad#124.
2. **`.gitmodules` state** (§3) -- finish #69.
3. **`backups2datalad plan`** -- needs only the superdataset checkout and
   one `/dandisets/?embargoed=true` listing.  Compares against
   `.gitmodules` (new id; `modified` newer than `dandiset-last-modified`;
   untagged release; embargo status vs `github-access-status`; stats row
   stale), applies `--exclude` and the quiescent period, lists ids in
   `.gitmodules` no longer on the server, and prints JSON with `public`,
   `embargoed`, `deleted`, each entry carrying id, reason, stats and
   `backup-runner`, ordered as in §5.3.  On drogon, `update-from-backup`
   without ids uses the same planner, which already removes the ~1400
   `/versions/` calls per run.
4. **Install on demand** (`update-from-backup --install-from github`, name
   TBD): clone registered-but-absent Dandisets from their `.gitmodules` URL,
   rename `origin` → `github`, `annex.private=true` before `git annex init`,
   `enableremote` `dandiapi` (and `datalad` when embargoed); the same for
   Zarrs from `zarr_gh_org` into a scratch `zarr_root`; default to
   `zarr_mode=asset_checksum` so an unchanged Zarr is never cloned (a big
   Zarr checkout is millions of symlinks, and `ZarrSyncer.run()` lists every
   file before deciding anything, `zarr.py:148`); drop each mirror after
   pushing so one job can stream through many on a 14 GB runner disk.
5. **Split "sync" from "record"**: `update-from-backup --no-superds` syncs
   and pushes the given ids and writes one JSON result per id (new commit,
   `.gitmodules` keys, commit subjects for the superdataset message);
   `record` applies results to a superdataset checkout *without*
   subdatasets (`git update-index --cacheinfo 160000,<sha>,<id>`, the keys,
   the make-private pass for deleted ids), commits and pushes, retrying on a
   non-fast-forward.  `record --from-remotes <ids>` instead reads each
   subdataset's `HEAD` from GitHub (`git ls-remote`) and the keys from that
   commit -- for mirrors updated elsewhere (§5.4).
6. **Graceful time limit** (`--time-limit SECONDS`, §5.4): stop taking new
   assets once the deadline passes, commit what is done, push, exit 0 with
   the Dandiset reported as "partial".
7. **HTTPS instead of SSH** in CI: keep SSH URLs in `.gitmodules` and set
   `url."https://x-access-token:${TOKEN}@github.com/".insteadOf
   git@github.com:`.  A GitHub App installation token
   (`actions/create-github-app-token`) on `dandisets` + `dandizarrs`: scoped,
   short-lived, higher rate limits than a PAT.

## 5. The workflow

### 5.1 Shape

```yaml
on:
  schedule: [{cron: '*/15 * * * *'}]
  workflow_dispatch:
    inputs: ...            # §5.2
concurrency: {group: mirror, cancel-in-progress: false}

jobs:
  plan:     # ~1 min on ubuntu-latest: checkout superds without submodules,
            # `backups2datalad plan`; outputs the matrix, the deleted ids and
            # the ids to dispatch elsewhere (§5.4)
  sync:     # one job per Dandiset (or per shard, §5.3)
    needs: plan
    strategy: {fail-fast: false, max-parallel: 5, matrix: ...}
    timeout-minutes: 60    # backstop; the tool stops itself earlier (§5.4)
  record:   # checkout superds, download results, `backups2datalad record`,
            # push; writes a summary table to $GITHUB_STEP_SUMMARY
    needs: [plan, sync]
    if: always() && needs.plan.outputs.any == 'true'
```

Each Dandiset is its own job with its own log, status and duration -- the
per-Dandiset timing record we lack today comes for free from the Actions API
and the job summary.  A failing Dandiset fails its job without blocking the
others; `record` commits whatever succeeded and the run goes red.

### 5.2 Manual runs

`workflow_dispatch` inputs mirroring `update-from-backup` (so a flawed
detection can always be overridden):

| input            | → option              | notes                                                   |
| ---------------- | --------------------- | ------------------------------------------------------- |
| `ids`            | positional            | blank = `plan`; given = skip the staleness check        |
| `mode`           | `--mode`              | `timestamp` / `force` / `verify`                        |
| `zarr_mode`      | `--zarr-mode`         | default `asset_checksum` in CI                          |
| `force`          | `--force`             | `assets-update`                                         |
| `exclude`        | `--exclude`           | default `00(1412\|1411\|1449)$`, as the cron            |
| `gc_assets`      | `--gc-assets`         | boolean                                                 |
| `tags`           | `--tags/--no-tags`    | boolean                                                 |
| `runner`         | `runs-on`             | `hosted` / `drogon`                                     |
| `max_batch`      | `plan --max`          | §5.3                                                    |
| `extra_args`     | appended verbatim     | `--asset-filter`, `--quiescent-period`, `-l DEBUG`, ... |

`--force-push` stays out of the form (it overwrites history; do it from a
shell), and `--asset-filter` only via `extra_args`.  `extra_args` also keeps
us under the cap on `workflow_dispatch` inputs.

### 5.3 How much one run takes on

`max-parallel` caps only how many matrix jobs run *at once*; the run takes
on the whole matrix (≤ 256 jobs), queued behind that cap.  So `plan` also
caps the total (`--max N`, e.g. 20): a run finishes its batch in bounded
time and the next scheduled run picks up the rest, which is still stale in
`.gitmodules` and therefore still planned -- nothing is lost.

Order within the batch: smallest first by `dandiset-stats` (so small updates
are never stuck behind big ones), but always including the few Dandisets
that have been stale longest (so big ones cannot starve).  Beyond ~20 ids
(first run, `--mode force`, a mass re-publish) `plan` shards: N jobs, each
running the in-process `--workers` pool over its share.

### 5.4 Long-running backups

We regularly have one backup that runs for hours (001873 has been updating
on drogon for 12+ hours, with git-annex mostly idle -- an inefficiency to be
tracked down on its own).  Today it clogs the whole cron run.

What CI does with it: at `timeout-minutes` the job is killed.  That is
*safe* -- the fresh clone is discarded and nothing was pushed (with H3
handled) -- but it is wasted work, and the Dandiset would be picked again
next time and killed again.  Three layers, cheapest first:

1. **Make progress resumable.**  `--time-limit` (§4.6) stops taking new
   assets at the deadline, commits the segment done so far and pushes.  The
   asset listing is ordered by `created`, and the code already commits in
   segments (at version boundaries) and has a "will download in a future
   run" path for unhashed assets (`asyncer.py:175-230`), so a deadline is
   one more reason to set `downloading = False`.  One catch: when the
   listing finishes, the state file gets the server's `modified`
   (`asyncer.py:613`), which would mark a partial backup complete; with a
   deadline it must record the last processed asset's timestamp instead, so
   the next run resumes.  Most "long" Dandisets then simply finish over a few
   runs, each visible in CI.  (A single huge Zarr is not split this way --
   `ZarrSyncer` commits once at the end.)
2. **Route what CI cannot finish** to a self-hosted runner.  A Dandiset gets
   `backup-runner = drogon` in `.gitmodules` -- by hand, or by `record` after
   N consecutive deadline hits without progress.  `plan` does not put it in
   the matrix; it `workflow_dispatch`es a separate `sync-one.yml` run for
   it with `concurrency: {group: dandiset-<id>, cancel-in-progress: false}`,
   so it can run for as long as it takes (self-hosted jobs may run 5 days),
   never twice at once, and never holds up the scheduled runs.
3. **Record asynchronously.**  `sync-one.yml` pushes the subdataset to GitHub
   and, as its last step, runs `record --from-remotes <id>` itself (retrying
   the superdataset push against concurrent `record`s).  If that step is
   lost, the next `plan` notices the subdataset's GitHub `HEAD` differs from
   the gitlink (`git ls-remote`, only for Dandisets with `backup-runner`) and
   records it -- the scheduled run just queries the state; it never waits on
   the long job.

This is the same split dandi-compute uses (§8): the scheduled workflow only
*starts* long work, its state lives in the data (there `jobs.tsv` on the
archive, here `.gitmodules` + the subdataset remotes), and a separate step
reconciles it.

### 5.5 Other differences from cron

* **Trigger.**  `schedule: '*/15 * * * *'` until the archive can tell us
  what changed: a webhook from the dandi-archive instance on Dandiset
  modification/publication/unembargo → `repository_dispatch` (GitHub) or the
  equivalent Forgejo webhook, carrying the Dandiset id(s), so a run starts
  within seconds and `plan` confirms against the API.  The schedule then
  stays as a slow safety net (e.g. hourly) for missed notifications.
* `schedule` is best-effort: runs are routinely 5-30 min late and dropped
  under load.  dandi-compute avoids that by dispatching from a site cron
  (`launcher/dispatch_github_action_cron.sh`), and only when there is work;
  for us the `plan` job is ~1 min on a hosted runner, so either is fine.
  `concurrency` prevents overlaps, which the cron does not.
* The `GitHubGate` is per process, so five parallel jobs are five gates.
  `max-parallel` is the only global throttle: keep it low and let each gate
  still handle 429/403 cooldowns.
* Hosted runners: 6 h per job, ~14 GB free disk, 7 GB RAM.
* git-annex ≥ 10.20240430 on the runner (`datalad-installer git-annex -m
  datalad/git-annex:release`, or conda-forge), cached.
* Pin backups2datalad to a tag/commit instead of `pip install` of `master`
  every run; bump the pin by PR, so a bad merge cannot take the mirror down
  unnoticed.
* Failure notification with a cooldown (dandi-compute's `check-cooldown`
  action: one mail per 24 h per workflow), so a stuck Dandiset does not mail
  every 15 minutes.

## 6. Embargoed Dandisets

`plan` separates them; they must not be synced by a workflow in a public
repository.  Proposal: a **private** repository (e.g.
`dandi/dandisets-private`) holding only the workflow -- not a clone of the
superdataset -- whose `plan` job takes the `embargoed` list and whose `sync`
jobs push the private subdatasets.  Its `record` step pushes to the public
superdataset, which today already carries the private submodules' gitlinks
and `github-access-status = private`, so nothing new is revealed.

Minutes: GitHub-hosted runners on private repositories draw from the
organization's plan quota (a few thousand minutes/month on Free/Team,
unlimited for public repos); self-hosted runners cost no minutes at all.
Embargoed Dandisets are few and change rarely, so either fits; a
self-hosted runner on drogon/typhon registered to the private repo only
also keeps their data off shared infrastructure.  Worth asking GitHub for
a nonprofit/education plan for `dandi` if hosted minutes get tight.

## 7. Self-hosted runners: what runs where

Two separate uses, not to be confused:

* **Now, visibility only** (optional stepping stone): register drogon as a
  runner and have a scheduled workflow run today's `update-cron` script there,
  on drogon's full clone.  Same work as the cron; the only gain is logs and
  status in Actions.  Worth it only if §4 takes long.
* **Target**: all per-Dandiset jobs work in scratch clones (install on
  demand), wherever they run.  Hosted runners take the bulk; `runs-on:
  [self-hosted, drogon]` takes the `backup-runner = drogon` ones (§5.4).  The
  superdataset of record is the one on GitHub, updated by `record`;
  `/mnt/backup/dandi/dandisets` on drogon becomes a consumer that `git pull`s
  (and installs subdatasets as needed) for the other tools in §2.3.

Either way, a runner executes workflow code from the repo it serves, so
self-hosted runners only for `schedule`/`workflow_dispatch` on the default
branch -- dandi-compute disables PRs from forks for exactly this reason --
and a hosted "is the runner online?" pre-check job so a dead runner fails
fast instead of queueing forever (dandi-compute's `check-runner` action).

## 8. What dandi-compute does (reviewed 2026-09-24)

`dandi-compute/dandi-compute-runner` + `dandi-compute-core`, running on MIT
Engaging:

* **Public repo for logs, self-hosted runners for work.**  Runners run as
  SLURM jobs on the HPC; forks' PRs are disabled; a hosted pre-check fails the
  run if the runner is offline.
* **Trigger from the site, only when needed.**  A login-node cron (every 15
  min, `flock`ed) calls `dandicompute jobs pending` and only then
  `workflow_dispatch`es `process-queue.yml`.
* **The workflow only dispatches.**  It submits SLURM array jobs and
  returns; the long work (up to 48 h) runs outside the Actions job, and
  `workflow_run`-triggered "Refresh state" reconciles afterwards.
* **State lives with the data**: `derivatives/jobs.tsv` on the archive, one
  row per job with `status` (`pending`/`stalled`/`failed`/`successful`),
  submission/completion times, durations.  Jobs *claim* work by writing a
  marker before running, so overlapping dispatches never double-run it; a
  requeued job finds its own claim and resumes.
* **Timeouts end as `failed`, not `stalled`**: jobs get a signal ahead of
  their time limit and upload their logs.
* Email on failure, at most once per 24 h per workflow.

Adopted above: state in the data plus reconciling (§5.4.3), per-item
concurrency as the "claim" (§5.4.2), graceful deadline (§5.4.1),
runner-online pre-check and fork policy (§7), notification cooldown (§5.5),
optional site-side dispatch (§5.5).

## 9. Forgejo + aneksjo later

* Forgejo Actions reads the same YAML (`runs-on` labels differ; `uses:`
  resolves against `data.forgejo.org` mirrors); matrices, `concurrency`,
  artifacts are supported.
* `Manager.gh` / `GitHub` in `manager.py` is the only forge API client
  (create repo, visibility, description, releases); the rest is plain `git`
  and `datalad push`.  Put a small `Forge` protocol in front of it with a
  Forgejo (Gitea API) implementation, and make the sibling name (`github`,
  hard-coded throughout `adataset.py`/`syncer.py`/`zarr.py`) configurable.
* With aneksjo the forge can hold annex content, which would make the
  (currently dead) `populate` step meaningful again as a push to the same
  sibling.

## 10. Order of work

1. ~~H1/H2 guard~~ → dandi/backups2datalad#124.
2. `.gitmodules` state (§3), by finishing #69 -- separate session.
3. `plan` (§4.3), used by `update-from-backup` on drogon too.
4. Install on demand + `annex.private` + HTTPS pushes (§4.4, §4.7); test on
   drogon against an empty scratch checkout for a handful of ids.
5. Graceful `--time-limit` (§4.6).
6. `--no-superds` / `record` (§4.5); workflow on `dandi/dandisets` for
   public Dandisets, with drogon's cron still running under `-e` for the
   rest; `backup-runner` routing (§5.4).
7. Private repo for embargoed Dandisets (§6); retire the cron.
8. `Forge` abstraction for Forgejo (§9).

## 11. Open questions

* `annex.private`, or reuse drogon's UUID in CI (`annex.uuid` set before
  `init`) so `whereis` history stays meaningful?
* Is `backup-runner` set by hand only, or automatically after N deadline hits
  without progress -- and what counts as progress for a single huge Zarr?
* Do we want `record` to batch (one superdataset commit per run, as today)
  or accept one commit per `sync-one.yml` run for routed Dandisets?

## 12. Changes since v1

* The separate `.dandi/mirrors.tsv` index is gone: per-Dandiset state goes
  into `.gitmodules`, extending #69 (§3), including Zarr counts.
* Checked the history behind the `/versions/` call and the description cache
  (§3.2, §3.3), and measured from the superdataset history that publish and
  unembargo both bump the draft's `modified`.
* Added `workflow_dispatch` inputs mirroring the CLI (§5.2), a per-run cap
  and size-based ordering (§5.3), and a design for long-running backups
  (§5.4) informed by dandi-compute (§8).
* Replaced "keep embargoed Dandisets on drogon" with a private repository
  (§6); clarified the two uses of self-hosted runners (§7).
* H1/H2 fixed in #124.
