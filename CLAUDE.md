# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

backups2datalad is a Python tool for mirroring Dandisets (datasets from the DANDI neuroscience data archive) and their Zarr files as git-annex repositories. It works with the DANDI API to fetch metadata and data, creating local mirrors that can be pushed to GitHub organizations.

The tool handles both public and embargoed Dandisets. Embargoed Dandisets are mirrored as private GitHub repositories, which are automatically converted to public when they are unembargoed.

## Development Environment Setup

### Prerequisites

- Python 3.10+
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

## Zarr Backup Performance

Zarrs routinely have tens of thousands of entries, so anything done once per
entry dominates the runtime.

- `AsyncAnnex` (`annex.py`) talks to long-running `git annex --batch`
  subprocesses.  Use the plural methods (`mkkeys()`, `get_keys_remotes()`,
  `from_keys()`, `register_urls()`), which *pipeline* their requests: a whole
  chunk (`BATCH_CHUNK_SIZE`) is written by one task while the responses are read
  concurrently by another.  The singular wrappers (`mkkey()`, `from_key()`, ...)
  are one-item calls on top of the same machinery and pay a full round trip per
  item, so never call them in a loop over Zarr entries.
- `ZarrSyncer.update_entries()` (`zarr.py`) therefore runs the registration as
  four whole-list phases (make keys → whereis → fromkey → registerurl) rather
  than five round trips per entry.  It logs progress once per chunk instead of
  once per URL; the per-chunk timestamps make it obvious which phase is slow.
- Measured with `tools/bench-zarr-registration` (git-annex 10.20240129): the
  batched path is ~1.5x the per-entry loop (8.5 vs 13.7 ms/entry), and the
  per-entry cost is *flat* from n=500 to n=20,000 -- nothing here is
  superlinear in the number of entries.  After batching, `examinekey` is
  essentially free (0.03 ms/entry) and `fromkey` dominates at ~5.7 ms/entry,
  which is git-annex's own work.  A production backup running far slower than
  ~9 ms/entry is hitting something environmental, not this code path.
- `registerurl` runs with `annex.alwayscompact=false`, and git-annex only
  commits its journal to the git-annex branch when the process exits, so a
  batch process kept alive for a whole Zarr accumulates one journal file per
  key in the flat `.git/annex/journal/`.  This *looks* like it should degrade,
  and an earlier version of this code restarted the process periodically to
  bound it -- but `tools/bench-zarr-registration` measured `registerurl` flat
  at ~1.7 ms/entry from 16,000 to 40,000 journal files, while the restarts
  themselves cost +104% at n=8,000 and +139% at n=20,000 (each one forces a
  git-annex branch commit, and they get more expensive as the branch grows).
  Don't re-add that without measuring first.
- The `whereis` lookup only exists to log "not in backup remote", so it is
  skipped entirely when no backup remote is configured.
- Pipelining means a desynchronised response stream would misattribute a whole
  chunk rather than a single response, so two guards exist: `render_request()`
  rejects a request containing an embedded newline (S3 object keys may contain
  one), and `mkkeys()` checks each returned key against the size and digest it
  asked for -- `examinekey` output is a bare key, so unlike the `--json`
  commands a shifted response would otherwise parse fine and silently annex
  files under the wrong keys.  A batch call that fails discards its subprocess
  instead of reusing one whose stdout still holds unread responses.
- Failures inside a batch call surface as an `ExceptionGroup` (anyio task
  group) rather than the bare exception the one-at-a-time code raised.

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
- `AsyncDataset.push()` accepts `force` parameter in `adataset.py:362`
- `BackupConfig.force_push` stores which repositories to force-push
- Helper methods `should_force_push_dandisets()` and `should_force_push_zarrs()` in `config.py`
- Push call sites in `datasetter.py:240`, `datasetter.py:396`, and `zarr.py:597` check config

## Important Environment Variables

- `DANDI_API_KEY`: Required API token for the DANDI instance being mirrored
- `GITHUB_TOKEN`: Optional GitHub access token for pushing to GitHub (preferred over git config)
