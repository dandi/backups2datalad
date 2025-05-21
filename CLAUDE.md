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

## Important Environment Variables

- `DANDI_API_KEY`: Required API token for the DANDI instance being mirrored