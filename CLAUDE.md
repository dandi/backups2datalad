# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

backups2datalad is a Python tool for mirroring Dandisets (datasets from the DANDI neuroscience data archive) and their Zarr files as git-annex repositories. It works with the DANDI API to fetch metadata and data, creating local mirrors that can be pushed to GitHub organizations.

The tool handles both public and embargoed Dandisets. Embargoed Dandisets are mirrored as private GitHub repositories, which are automatically converted to public when they are unembargoed.

## Development Environment Setup

### Prerequisites

- Python 3.11+
- git-annex version 10.20240430 or newer
- DANDI API token (set as environment variable `DANDI_API_KEY`)
- For pushing to GitHub, a GitHub access token via one of:
  - `GITHUB_TOKEN` environment variable (preferred)
  - `hub.oauthtoken` key in `~/.gitconfig` (fallback)

### Installation

```bash
# Install in development mode
pip install -e .
```

## Common Commands

### Running Tests

```bash
# Run all tests
tox

# Run specific test environment
tox -e lint        # Run linting checks
tox -e typing      # Run type checking
tox -e py3         # Run Python tests

# Run a specific test file
pytest test/test_core.py

# Run a specific test
pytest test/test_core.py::test_1
```

Before committing code, make sure that typing check passes.

### Linting and Type Checking

```bash
# Run linting checks
flake8 src test

# Run type checking
mypy src test
```

## Architecture Overview

backups2datalad is structured around these key components:

1. **Command Line Interface**: Implemented using `asyncclick` for async operations, defined in `__main__.py`.

2. **Configuration**: `BackupConfig` class in `config.py` handles loading and validation of configuration settings from YAML files.

3. **Core Components**:
   - `DandiDatasetter` in `datasetter.py`: Main class for mirroring operations
   - `AsyncDandiClient` in `adandi.py`: Async client for interacting with DANDI API
   - `AsyncDataset` in `adataset.py`: Wrapper around DataLad Dataset for async operations
   - `Syncer` in `syncer.py`: Handles synchronization of assets

4. **Manager and GitHub Integration**: `Manager` class with GitHub API integration for pushing repositories.

5. **Zarr Support**: Special handling for Zarr files, with checksumming and specialized mirroring.

## Embargo Handling

The system supports working with both public and embargoed Dandisets:

1. **Embargoed Dandisets**:
   - Stored in git-annex with embargo status tracked in `.datalad/config`
   - When pushed to GitHub, they are created as private repositories
   - Special handling for authentication when accessing embargoed Dandisets

2. **Unembargoed Dandisets**:
   - When a Dandiset is unembargoed, the system updates its status
   - GitHub repositories are converted from private to public
   - S3 URLs for assets are registered with git-annex

3. **Status Tracking**:
   - The embargo status of a Dandiset is tracked and synchronized between the remote server and local backup
   - GitHub repository access status (private/public) is stored in the superdataset's `.gitmodules` file

### Zarr Embargo Handling ("Zarrbargo")

Zarr files within embargoed Dandisets receive special handling to maintain privacy:

1. **Embargo Propagation**:
   - When a Zarr file is backed up from an embargoed Dandiset, the parent Dandiset's embargo status is propagated to the Zarr repository
   - Implementation: `DandiDatasetter.backup_zarr()` in `datasetter.py` fetches parent embargo status and passes it to `sync_zarr()`
   - The Zarr dataset's embargo status is set in `.datalad/config` via `AsyncDataset.set_embargo_status()`

2. **GitHub Privacy Settings**:
   - If `zarr_gh_org` is configured, Zarr repositories are created on GitHub with privacy matching their embargo status
   - Embargoed Zarrs → private GitHub repositories
   - Public Zarrs → public GitHub repositories
   - Implementation: `sync_zarr()` in `zarr.py` creates GitHub siblings with appropriate privacy

3. **Metadata in `.gitmodules`**:
   - The superdataset's `.gitmodules` file tracks the privacy status of each Zarr submodule
   - Custom attribute: `submodule.<path>.github-access-status` set to either "private" or "public"
   - This metadata is set when the Zarr is added as a submodule in `DandiDatasetter.backup_zarr()`
   - Purpose: Provides a declarative record of expected GitHub repository privacy state

4. **Unembargo Transition**:
   - When a Dandiset transitions from embargoed to open, all associated Zarr repositories are updated
   - Implementation: `Syncer.update_embargo_status()` in `syncer.py` triggers `update_zarr_repos_privacy()` after making the main Dandiset public
   - Process:
     1. Identifies Zarr submodules by scanning `.gitmodules` for paths ending in `.zarr` or `.ngff`
     2. Updates each Zarr's GitHub repository to public via GitHub API
     3. Updates `.gitmodules` to set `github-access-status=public` for all Zarr submodules
     4. Commits the `.gitmodules` changes
   - Error handling: Individual GitHub API failures are logged but don't block other Zarrs from being updated

5. **Identification of Zarr Submodules**:
   - Current approach: Path-based detection using file extensions (`.zarr`, `.ngff`)
   - Implementation: `Syncer.update_zarr_repos_privacy()` in `syncer.py`
   - Limitation: May not catch Zarr files with non-standard naming conventions

6. **Configuration Requirements**:
   - Both `gh_org` (for Dandisets) and `zarr_gh_org` (for Zarrs) must be configured for privacy updates to occur
   - If either is missing, Zarr privacy updates are skipped gracefully

### Key Implementation Components

- `DandiDatasetter.backup_zarr()` in `datasetter.py` - Embargo status propagation to Zarr sync and setting `github-access-status` in `.gitmodules`
- `Syncer.update_embargo_status()` in `syncer.py` - Triggering Zarr updates during unembargo
- `Syncer.update_zarr_repos_privacy()` in `syncer.py` - Batch updating Zarr repository privacy
- `sync_zarr()` in `zarr.py` - Creating Zarr repos with embargo-aware privacy

## Git vs. git-annex Content Policy

Dandiset mirrors get their `.gitattributes` from the `cfg_dandiset` DataLad
procedure in `src/backups2datalad/procedures/cfg_dandiset.py` (a fixed-up
replacement for DataLad's `cfg_text2git`, which put *all* text files into Git
regardless of size):

- Text files up to `BACKUPS2DATALAD_TEXT_SIZE_LIMIT` (default `10MiB`) go into
  Git; binary files and anything above the limit go to git-annex.  This holds
  for the metadata we maintain (`dandiset.yaml`, `.dandi/`) as well -- there
  are no exceptions to the rule.
- The rule is written as a block delimited by `### BEGIN dandiset default
  policy (backups2datalad)` / `### END ...` markers; only that block is
  managed, and rules after it override the policy.
- `annex.dotfiles=true` is set alongside it (`ensure_dotfiles()`, recorded via
  `git annex config` in the `git-annex` branch).  It is load-bearing: git-annex
  otherwise adds dotfiles and dot-directory content to Git regardless of
  `annex.largefiles`, so `.dandi/assets.json` would never be annexed.  No
  `.gitattributes` rule can override that -- 000026 has carried such a
  workaround since 2022 and its 67 MiB `assets.json` is still in Git.

Key points:

- The module is stdlib-only: DataLad runs it as a plain script, and
  `AsyncDataset` imports `apply_policy()` from it.
- `AsyncDataset.ensure_installed()` passes `-c dandiset` (plus
  `datalad.locations.extra-procedures`) to `datalad create`, and reapplies the
  policy via `AsyncDataset.ensure_dandiset_policy()` on already-existing
  datasets, so mirrors made under the old policy are migrated on the next
  backup run.  Dating the commits is the caller's job (the procedure just
  commits with the ambient `GIT_*` environment): `ensure_installed()` passes
  `custom_commit_env(commit_date)` down to `datalad create`, and
  `ensure_dandiset_policy()` dates the migration commit the same as the
  then-current HEAD so that a mirror's timeline does not jump into the
  present.  Zarr datasets pass `cfg_proc=None` and are not affected.
- `Syncer`/`asyncer.py` uses the same limit (`size_limit_bytes()`) when
  deciding whether to register an asset with git-annex instead of downloading
  it into Git.

## Main Workflow

1. Configuration is loaded from a YAML file
2. DANDI API client is initialized with an API token
3. The mirroring command (e.g., `update-from-backup`) is executed, which:
   - Fetches Dandiset metadata from the DANDI API
   - Creates or updates local git-annex repositories
   - Sets appropriate embargo status for each Dandiset
   - Synchronizes assets between DANDI and local repositories
   - Optionally pushes changes to GitHub organizations (with appropriate privacy settings)
   - Creates tags for published versions

## Quiescent Period

A Dandiset that was modified on the server a moment ago is likely still being
changed -- a mass upload or delete of assets is in flight -- and mirroring it
mid-flight makes the asset listing we paginate through disagree with the assets
we later query (see issue #119, where 1059 assets vanished between the listing
and the `assets.json` comparison, tripping `UnexpectedChangeError`).

`DandiDatasetter.update_dandiset()` therefore skips any Dandiset whose
`version.modified` is fewer than `BackupConfig.quiescent_period` seconds in the
past, before the local dataset is even created; the next run picks it up.

- Default: `DEFAULT_QUIESCENT_PERIOD` (30 s) in `consts.py`.
- Settable per-run via `--quiescent-period SECONDS` on `update-from-backup`, or
  via `quiescent_period` in the config file.  `0` disables the check.
- The arithmetic lives in `util.quiescence_wait()`, which returns how many more
  seconds must elapse (non-positive means quiescent).  A `modified` in the
  future -- a local clock lagging the server's -- lengthens the wait, erring
  towards skipping.
- The config field uses `default_factory` so that the constant is read at
  instantiation time; `test/conftest.py`'s autouse `no_quiescent_period` fixture
  patches it to `0` for the suite, since sample Dandisets are created and backed
  up within seconds of each other.  Tests exercising the gate itself
  (`test/test_quiescence.py`) pass `quiescent_period` explicitly.

## Dirty Mirrors

A mirror left uncommitted by an interrupted run used to be able to hide
itself.  `AsyncDataset.set_assets_state()` writes `.dandi/assets-state.json`
and stages it before the commit that seals it, and in the "listing consumed"
branch it writes the server's `version.modified` verbatim (`asyncer.py`).  A
run cut short between there and `sync_dataset()`'s final commit therefore left
a working-tree state file claiming a backup that was never committed --
alongside the staged deletions from `prune_deleted()` and a staged
`dandiset.yaml`.  `update_dandiset()` read that file, concluded the backup was
current, skipped `sync_dataset()`, and with it the only dirtiness check there
is; the Dandiset then went unmentioned in every subsequent run's log and never
failed one, so no one was told (see 000571, which sat this way from
2026-09-10).  The window that produces this state is also the only one in
which the state file already holds `version.modified`, so the failure reliably
conceals itself.

`update_dandiset()` now gates on `AsyncDataset.get_backup_state()`, which is
the **older** of the states recorded in the working tree and in `HEAD` (`None`
-- sync, do not skip -- if either is missing).  An uncommitted bump therefore
cannot make a mirror look current: the gate sees the committed timestamp, the
Dandiset looks stale, `sync_dataset()` runs, and the `is_dirty()` there reports
it with `describe_dirt()`.

Deliberately, no dirtiness check runs on the skip path: `git status` is ~70 ms
warm on a 50k-file mirror (far worse cold) and even `git diff --cached` is
~8 ms, neither of which is worth paying for ~1400 mirrors every run.
`get_backup_state()` costs one `git cat-file` (~2 ms); the working-tree read is
a plain file read.

What this deliberately does *not* catch: staged or unstaged junk on a mirror
whose recorded state is current on both sides, since nothing then makes it look
stale.  Everything an interrupted sync leaves behind does make it look stale,
because the sync writes the state last.  For the rest there is
`tools/find-INVISIBLE-changed.sh` on drogon, a fleet-wide sweep
(`git diff-index --cached --quiet HEAD` per mirror, ~12 s for all of them).

Two notes for anyone extending this:

- Comparing the state timestamp against `get_last_commit_date()` instead is
  tempting -- commit author dates are DANDI timestamps (`custom_commit_env()`)
  and `sync_dataset()`'s final commit is dated `version.modified` -- but `git`
  stores commit dates in whole seconds while the state file carries
  microseconds, so that comparison needs a sub-second tolerance or it flags
  every mirror.  Reading the blob from `HEAD` avoids the question.
- Once a mirror is in this state, a full sync would also hit
  `dump_asset_metadata()`'s garbage-collection error: `prune_deleted()` removed
  the files without rewriting `assets.json`, so their metadata is left with
  neither a local file nor a server asset and `prune_metadata()` reports it.
  That error has no threshold -- one entry raises unless `gc_assets` is set --
  and, unlike ordinary deletions (which `get_deleted()` handles by popping the
  metadata first), it means the two records disagree.

## Testing

The project uses pytest for testing, with fixtures for:
- Setting up Docker-based DANDI instances
- Creating sample Dandisets
- Managing temporary directories

The tests verify:
- Proper syncing of Dandisets
- Creation and updating of local repositories
- Handling of published versions and tagging
- Error conditions and edge cases
- Embargo status handling

### AI-Generated Tests

When adding new tests generated by AI assistants, mark them with `@pytest.mark.ai_generated`:

```python
@pytest.mark.ai_generated
async def test_my_new_feature() -> None:
    """Test description."""
    # test code
```

This allows filtering or identifying AI-generated tests separately if needed.

## Force-Push Feature

When repositories need to be rebuilt from scratch (e.g., after history rewrites), the `--force-push` option allows overwriting remote Git history on GitHub:

```bash
# Force-push Dandisets only
backups2datalad update-from-backup --force-push dandisets 000874

# Force-push Zarrs only
backups2datalad update-from-backup --force-push zarrs 000874

# Force-push both Dandisets and Zarrs
backups2datalad update-from-backup --force-push all 000874

# Can specify multiple times
backups2datalad update-from-backup --force-push dandisets --force-push zarrs
```

**Warning**: Force-pushing overwrites remote Git history! Use with caution.

Implementation:
- `AsyncDataset.push()` accepts a `force` parameter (`adataset.py`)
- `BackupConfig.force_push` stores which repositories to force-push
- Helper methods `should_force_push_dandisets()` and `should_force_push_zarrs()` in `config.py`
- The three push call sites (`DandiDatasetter.update_dandiset()`,
  `DandiDatasetter.tag_releases()`, `sync_zarr()`) check the config

Known gap: `push()` passes a bool where DataLad's `push` expects
`force="gitpush"`/`"all"`, so the flag currently does not force anything
(see `docs/github-zarr-rate-limits-plan.md` §11).

## GitHub Rate Limits

All GitHub API mutations (repo creation, `PATCH`, releases) go through one
`GitHubGate` per process (`aioutil.py`, held by `GitHub.gate` in `manager.py`):
serialised ≥ 1 s apart, with a cooldown shared by every worker whenever GitHub
answers 429, or 403 with `retry-after` / `x-ratelimit-remaining: 0` / a
secondary-limit message.  The cooldown is whatever GitHub says (`retry-after`,
else `x-ratelimit-reset`), else GitHub's documented fallback of a minute
doubling per consecutive hit; after `GITHUB_RATE_LIMIT_ATTEMPTS` slept-out
hits the gate gives up and mutations raise `GitHubRateLimited`.  Constants
live in `consts.py`; there are no config/CLI knobs.  `arequest(gate=...)` is
opt-in -- without a gate (DANDI API, S3) it behaves as before.

`AsyncDataset.create_github_sibling()` still uses DataLad's
`create_sibling_github` but inspects its result records itself, passes a real
`description=`, and retries a rate-limited creation through the gate.
`sync_zarr()` converges on every visit: it pushes whenever HEAD has commits
the `github` sibling lacks (`has_unpushed_commits()`, a plain push) and
records the description whenever the `dandi.github-description` cache is
missing; `Manager.set_zarr_description(..., ds=)` must be given the dataset
actually on disk.  Dirty datasets remain a hard error whose message carries
`describe_dirt()`.  Design and review trail:
`docs/github-zarr-rate-limits-plan.md`.

## Important Environment Variables

- `DANDI_API_KEY`: Required API token for the DANDI instance being mirrored
- `GITHUB_TOKEN`: Optional GitHub access token for pushing to GitHub (preferred over git config)
