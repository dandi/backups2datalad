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
- For pushing to GitHub, a GitHub access token stored in the `hub.oauthtoken` key in `~/.gitconfig`

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

## Important Environment Variables

- `DANDI_API_KEY`: Required API token for the DANDI instance being mirrored
