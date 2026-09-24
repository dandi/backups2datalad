# Plan: running the 15-minute mirror update in CI

Status: **draft for discussion** (2026-09-24).  Nothing here is implemented
yet.  Line references are against `main` at `90aacaf`.

Goal: move the `backups2datalad-update-cron` invocation (every 15 min on
drogon) into GitHub Actions on `dandi/dandisets` -- and later Forgejo
Actions on a forgejo+aneksjo instance -- so that each run starts from a
fresh checkout of the superdataset, installs **only** the Dandisets (and
Zarrs) that need work, processes them in parallel, and makes that activity
visible as CI jobs.

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
config), and the `-108` scripts are one-offs.

## 2. Where the code assumes a full local clone

Ordered by how badly each bites in a fresh CI checkout.

### 2.1 Hazards (would corrupt or duplicate mirrors)

* **H1 -- an uninstalled Dandiset is silently re-created.**
  `update_dandiset()` always calls `init_dataset()` →
  `AsyncDataset.ensure_installed()` (`datasetter.py:220`,
  `adataset.py:85`), which runs `datalad create` whenever the directory is
  not an installed dataset.  In a checkout where `000026/` is just an empty
  submodule directory, that creates a *new* dataset (new datalad-id, one-commit
  history), then `create_github_sibling()` finds the existing repo and the
  push is rejected -- or, with `--force-push`, overwrites it once the known
  `force=` gap in §11 of the rate-limits plan is fixed.  This is worth
  guarding against **today**, independently of CI: `rm -rf` of a mirror on
  drogon has the same effect.
* **H2 -- same for Zarrs.**  `sync_zarr()` calls `ensure_installed()` on
  `zarr_root / <zarr_id>` (`zarr.py:522`, reached from `asyncer.py:352` for
  *every* Zarr asset of a synced Dandiset in the default `timestamp`
  `zarr_mode`).  Without `/mnt/backup/dandi/dandizarrs` every Zarr would be
  re-created from scratch and re-listed from S3.
* **H3 -- a fresh git-annex repo per run.**  Every CI clone that runs
  `git annex init` records a new UUID (plus `describe here`,
  `datasetter.py:626`) in the `git-annex` branch, which is then pushed.
  Every run would add another dead repository to every touched mirror.  The
  mirrors never hold annexed content (blobs are `fromkey --force` +
  `registerurl`, `annex.py:64`; text ≤ 10 MiB goes to Git), so CI clones
  should set `annex.private=true` before `git annex init`.
* **H4 -- public logs of embargoed Dandisets.**  `dandi/dandisets` is public,
  so its Actions logs and artifacts are world-readable.  Syncing an embargoed
  Dandiset logs asset paths, Zarr ids, and commit messages.  Embargoed
  Dandisets must not be processed by a workflow in a public repository (see
  §4.4).

### 2.2 Per-run work that needs every mirror present

For **every** Dandiset in the API listing, every 15 minutes:

* `get_backup_state()` -- two local reads of `.dandi/assets-state.json`
  (working tree and `HEAD`).  Cheap, but needs the clone.
* `tag_releases()` (`datasetter.py:256`, `:414`) -- one paginated
  `/versions/` API call **plus** a `git tag -l` per published version, even
  when nothing was published.  ~1400 API calls per run and the main reason
  the loop needs every clone.  The `/dandisets/` listing already carries
  `most_recent_published_version`, which is enough to decide whether there
  is anything to tag.
* `get_stats()` (`datasetter.py:261`) -- reads the `dandi.stats` cache from
  the subdataset's `.git/config` (`scope="local"`, `adataset.py:1014`), which
  a fresh clone does not have, so it would fall back to counting all files --
  and for Zarr submodules to `get_zarr_sub_stats()`, which needs
  `zarr_root/<id>` on disk.
* `ensure_github_remote()` -- assumes a `github` remote; a `datalad get -n`
  clone has `origin`.
* `ensure_dandiset_policy()` -- runs on every installed dataset (cheap, but
  may commit; must then be pushed).

After the loop:

* `set_superds_description()` (`datasetter.py:381`) sums
  `get_stored_stats()` over **all** subdatasets -- `.git/config` of each
  clone.  In CI this would silently report 0 bytes.
* `get_superds_commit_message()` runs `git log` inside each changed
  subdataset -- fine for the ones installed in this run.
* The "deleted Dandiset → make private" pass only reads `.gitmodules`; it
  already works on a bare checkout.

### 2.3 State that lives only on drogon

| State                                    | Where                                     | CI replacement                          |
| ---------------------------------------- | ----------------------------------------- | --------------------------------------- |
| Dandiset/Zarr file+size stats            | `dandi.stats` in each `.git/config`       | committed index (§3.1)                  |
| last GitHub description                  | `dandi.github-description`, `.git/config` | index, or accept one `GET` when changed |
| `dandi.populated`                        | `.git/config`                             | dead (no populate)                      |
| debug log per run                        | superds `.git/dandi/backups2datalad/`     | job logs + artifact (private only)      |
| Zarr mirrors                             | `/mnt/backup/dandi/dandizarrs/<id>`       | clone from `dandizarrs/<id>` on demand  |
| GitHub SSH key for private repos         | `~/.ssh` (`https_to_ssh_url`)             | `url.<https+token>.insteadOf git@…:`    |
| annex repo identity                      | one UUID per mirror on drogon             | `annex.private=true` (H3)               |

Other tooling that assumes the full tree (`tools/git-annex-info`,
`generate_whereis`, `du_dandisets`, `derivatives/`) is not part of the
15-minute loop and can stay on drogon.

## 3. Changes to backups2datalad

Each item is useful on drogon as well, so they can land before any CI
exists.

### 3.1 A committed per-Dandiset index in the superdataset

Add `.dandi/mirrors.tsv` (sorted by id, one line per Dandiset; TSV so that
`git diff` and merges stay line-local) to the superdataset:

```
id      commit   draft_modified                  latest_version  embargo  files  size
000026  3f2a…    2026-09-17T19:32:48.123456Z     0.230629.1734   OPEN     52341  12345678901
```

* `commit` is the subdataset commit the row describes -- equal to the
  gitlink in the same superdataset commit, so a stale row is detectable.
* `files`/`size` replace `dandi.stats` for the superdataset description and
  for `set_dandiset_description()`; Zarr stats go in the Dandiset row (a
  Zarr's size is only needed summed into its Dandiset).
* Written by whoever commits the superdataset (§3.4); bootstrapped once on
  drogon from the existing clones (`dandi.stats` + `assets-state.json` +
  `git tag`).

`.gitmodules` keys (like `github-access-status`) would also work, but every
run would then rewrite `.gitmodules`, which DataLad and every consumer of
the superdataset re-read.

### 3.2 `backups2datalad plan`

New subcommand that needs only the superdataset checkout and the API:

1. one paginated `/dandisets/?embargoed=true` listing;
2. compare each entry against `.dandi/mirrors.tsv`: new id; `draft_modified`
   newer; `most_recent_published_version` not yet tagged; embargo status
   changed; row stale; plus `--exclude`;
3. apply the quiescent period (the in-process check stays too);
4. report ids in `.gitmodules` but no longer on the server (the
   make-private pass);
5. print JSON: `{"public": [...], "embargoed": [...], "deleted": [...]}`,
   ready for `fromJSON()` in a workflow matrix.

On drogon, `update-from-backup` without ids can use the same planner
internally and drop the per-Dandiset `/versions/` calls, which already makes
the 15-minute run cheaper.

### 3.3 Install-on-demand ("lazy") mode

`update-from-backup --install-from github` (name TBD):

* if `<id>` is registered in `.gitmodules` but not installed: clone it from
  its `.gitmodules` URL (`datalad get -n`), rename `origin` → `github`, set
  `annex.private=true` before `git annex init`, `enableremote` the
  `dandiapi` (and, if embargoed, `datalad`) special remotes;
* if `<id>` is **not** registered: create it only after confirming the
  GitHub repository does not exist yet; otherwise error out;
* **never** `datalad create` a path that `.gitmodules` knows about, in any
  mode (fix for H1);
* the same for Zarrs: clone `zarr_gh_org/<zarr_id>` into a scratch
  `zarr_root` when absent, and refuse to create one whose GitHub repo exists
  (H2);
* prefer `zarr_mode=asset_checksum` in CI, so that a Zarr is only cloned when
  its asset's `modified` moved -- a large Zarr checkout is millions of
  symlinks, and `ZarrSyncer.run()` lists every file (`zarr.py:148`) before
  deciding whether anything changed;
* uninstall (drop `--reckless kill`) after pushing, so a single job can
  stream through many Dandisets on a 14 GB runner disk.

### 3.4 Split "sync one Dandiset" from "commit the superdataset"

`update_from_backup()` both syncs subdatasets and commits the superdataset.
For parallel jobs, split it:

* `update-from-backup --no-superds` (or a `sync` subcommand): sync and push
  the given ids; write one JSON result per id (new commit, index row,
  access status, the commit subjects used by `get_superds_commit_message()`).
* `record` subcommand: on a superdataset checkout **without** subdatasets,
  apply results with `git update-index --cacheinfo 160000,<sha>,<id>`, set
  `github-access-status`, rewrite `.dandi/mirrors.tsv`, run the make-private
  pass for deleted ids, commit, push; `set_superds_description()` sums the
  index.

drogon keeps calling `update-from-backup`, which becomes plan → sync →
record in one process.

### 3.5 Credentials without SSH

Keep the SSH URLs in `.gitmodules` (they are what users with access clone),
and in CI configure
`url."https://x-access-token:${TOKEN}@github.com/".insteadOf git@github.com:`
so the same code pushes over HTTPS.  A GitHub App installation token (via
`actions/create-github-app-token`) on `dandisets` and `dandizarrs` beats a
PAT: scoped, short-lived, and with higher rate limits.

## 4. The workflow

### 4.1 Shape

```yaml
on:
  schedule: [{cron: '*/15 * * * *'}]
  workflow_dispatch:
    inputs: {ids: {description: 'Dandiset ids (blank = plan)'}}
concurrency: {group: mirror, cancel-in-progress: false}

jobs:
  plan:        # ~1 min: checkout superds (no submodules), `backups2datalad plan`
    outputs: {ids: ..., deleted: ...}
  sync:        # one job per Dandiset
    needs: plan
    if: needs.plan.outputs.ids != '[]'
    strategy: {fail-fast: false, max-parallel: 5, matrix: {id: ${{ fromJSON(...) }}}}
    # checkout superds, install git-annex + backups2datalad (cached),
    # sync --install-from github <id>, upload result JSON as artifact
  record:      # checkout superds, download results, `backups2datalad record`, push
    needs: [plan, sync]
    if: always() && needs.plan.outputs.ids != '[]'
```

Each Dandiset is its own job with its own log, status, and timing, which is
the visibility asked for; `record` writes a job summary table
(`$GITHUB_STEP_SUMMARY`) of what was added/updated/tagged.  A failing
Dandiset fails its job without blocking the others; `record` commits whatever
succeeded and the run goes red.

### 4.2 Batching

A matrix is capped at 256 jobs, and a job costs ~30-60 s of setup.  `plan`
should shard when there are many ids (first run, `--mode force`, a mass
re-publish): e.g. ≤ 20 ids → one job each; otherwise N shards, each running
the in-process `--workers` pool.  A typical 15-minute window touches 1-3
Dandisets (see the superdataset log), so the common case is one job each.

### 4.3 Things GitHub Actions does differently from cron

* `schedule` is best-effort: runs are routinely 5-30 min late and are
  dropped under load.  Fine for a mirror; `concurrency` prevents overlap
  (the cron has no lock at all).
* The `GitHubGate` is per process; five parallel jobs are five gates.
  `max-parallel` is the only global throttle, so keep it low and let each
  gate still handle 429/403 cooldowns.
* Runner limits: 6 h per job, ~14 GB free disk, 7 GB RAM.  Dandisets with
  many or large Zarrs (000108, the LINC ones) should go to a self-hosted
  runner via a label chosen by `plan` (e.g. from a `heavy` list in the
  config).
* Needs git-annex ≥ 10.20240430 on the runner (`datalad-installer
  git-annex -m datalad/git-annex:release`, or conda-forge), cached.
* Pin backups2datalad to a commit/tag instead of `pip install` of `master`
  every run, so a bad push to backups2datalad cannot take the mirror down
  unnoticed; bump the pin by PR.

### 4.4 Embargoed Dandisets

`plan` separates them.  Options, in order of preference:

1. a **private** repo (e.g. `dandi/dandisets-embargoed-ci`) whose workflow
   does the same for the `embargoed` list and only pushes subdatasets;
   `record` in the public repo then updates only the gitlinks of the
   private submodules, which already happens today and reveals nothing new;
2. process them on a self-hosted runner with logging reduced to counts
   (`-l WARNING`, no per-asset lines) -- fragile, one log line away from a
   leak;
3. keep them on drogon's cron for now.

### 4.5 Self-hosted runner as a stepping stone

Registering drogon as a self-hosted runner for `dandi/dandisets` gets CI
visibility *before* any of §3 lands: a job that just runs today's
`update-cron` on drogon's existing clones.  Note the runner then executes
workflow code from the repo, so restrict it to `schedule`/
`workflow_dispatch` on the default branch, never `pull_request`.

## 5. Forgejo + aneksjo later

* Forgejo Actions reads the same YAML (`runs-on` labels differ; `uses:`
  resolves against `data.forgejo.org` mirrors); matrices, `concurrency`,
  artifacts are supported.
* `Manager.gh` / `GitHub` in `manager.py` is the only forge API client
  (create repo, visibility, description, releases); the rest is plain `git`
  and `datalad push`.  Put a small `Forge` protocol in front of it with a
  Forgejo (Gitea-API) implementation, and make the sibling name (`github`,
  hard-coded throughout `adataset.py`/`syncer.py`/`zarr.py`) configurable.
* With aneksjo the forge can hold annex content, which would make the
  (currently dead) `populate` step meaningful again as a push to the same
  sibling.

## 6. Suggested order

1. H1/H2 guard: never create a dataset that `.gitmodules` / the forge
   already has.  Small, and protects drogon now.
2. `.dandi/mirrors.tsv` + bootstrap on drogon; stats and superds description
   read from it.
3. `plan`; drop per-Dandiset `/versions/` calls on no-op runs.
4. Lazy install + `annex.private` + HTTPS-token pushes; test by running on
   drogon against an empty scratch checkout for a handful of ids.
5. `--no-superds` / `record` split; workflow on `dandi/dandisets` for public
   Dandisets with drogon's cron still handling embargoed ones (§4.4 option 3)
   -- run both with `-e` keeping them disjoint.
6. Private repo for embargoed; retire the cron.
7. `Forge` abstraction for Forgejo.

## 7. Open questions

* Does a publish or an unembargo always bump the draft's `modified`?  If so,
  `plan` needs only `draft_modified`; if not, the extra columns stay.
* Is `annex.private` enough, or do we want the existing drogon UUID reused
  (`annex.uuid` set before `init`) so `whereis` history stays meaningful?
* Which Dandisets are "heavy" enough for a self-hosted runner, and should
  that list be config or measured (asset count / Zarr count from the API)?
* Do the Zarr stats belong in the Dandiset row, or in a separate
  `.dandi/zarrs.tsv` so that the `dandizarrs` side can be planned
  independently (e.g. a separate `backup-zarrs` workflow)?
