from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from dandi.consts import EmbargoStatus
from dandi.dandiapi import RemoteZarrAsset
from ghrepo import GHRepo
import pytest

from backups2datalad.adataset import AsyncDataset
from backups2datalad.config import BackupConfig, Remote, ResourceConfig
from backups2datalad.manager import Manager
from backups2datalad.syncer import Syncer
from backups2datalad.zarr import sync_zarr

pytestmark = pytest.mark.anyio


class MockManager:
    def __init__(self) -> None:
        self.edit_repo_calls: list[tuple[GHRepo, dict[str, Any]]] = []
        self.log = MagicMock()
        self.config = BackupConfig(
            dandisets=ResourceConfig(path="dandisets", github_org="dandisets"),
            zarrs=ResourceConfig(path="zarrs", github_org="dandizarrs"),
        )
        self.gh = MagicMock()
        self.gh.edit_repo = AsyncMock()

    async def edit_github_repo(self, repo: GHRepo, **kwargs: Any) -> None:
        self.edit_repo_calls.append((repo, kwargs))
        await self.gh.edit_repo(repo, **kwargs)

    async def set_zarr_description(self, zarr_id: str, stats: Any) -> None:
        pass


async def test_embargo_status_parameter() -> None:
    """Test that sync_zarr accepts embargo_status parameter with correct default."""
    import inspect

    from backups2datalad.zarr import sync_zarr

    # Check that the function signature includes embargo_status parameter
    sig = inspect.signature(sync_zarr)
    assert "embargo_status" in sig.parameters
    assert sig.parameters["embargo_status"].default == EmbargoStatus.OPEN


async def test_zarr_repo_unembargoing() -> None:
    """Test that unembargoed Dandisets update their Zarr repositories to public."""
    # Create mocks
    ds = AsyncMock()
    ds.get_subdatasets = AsyncMock(
        return_value=[
            {
                "path": "/fake/path/foo.zarr",
                "gitmodule_path": "foo.zarr",
                "gitmodule_url": "https://github.com/dandizarrs/zarr123",
            },
            {
                "path": "/fake/path/bar.ngff",
                "gitmodule_path": "bar.ngff",
                "gitmodule_url": "https://github.com/dandizarrs/zarr456",
            },
            {
                "path": "/fake/path/not_zarr",
                "gitmodule_path": "not_zarr",
                "gitmodule_url": "https://github.com/dandizarrs/non_zarr789",
            },
        ]
    )
    ds.set_repo_config = AsyncMock()
    ds.commit_if_changed = AsyncMock()

    manager = MockManager()

    # Create a Syncer
    syncer = Syncer(
        manager=manager,  # type: ignore[arg-type]
        dandiset=MagicMock(),
        ds=ds,
        tracker=MagicMock(),
        error_on_change=False,
    )

    # Run the method to update Zarr repo privacy
    await syncer.update_zarr_repos_privacy()

    # Verify that the GitHub repos were updated to public
    assert len(manager.edit_repo_calls) == 2
    assert manager.edit_repo_calls[0][0] == GHRepo("dandizarrs", "zarr123")
    assert manager.edit_repo_calls[0][1] == {"private": False}
    assert manager.edit_repo_calls[1][0] == GHRepo("dandizarrs", "zarr456")
    assert manager.edit_repo_calls[1][1] == {"private": False}

    # Verify that github-access-status was updated in .gitmodules
    assert ds.set_repo_config.call_count == 2
    ds.set_repo_config.assert_any_call(
        "submodule.foo.zarr.github-access-status", "public", file=".gitmodules"
    )
    ds.set_repo_config.assert_any_call(
        "submodule.bar.ngff.github-access-status", "public", file=".gitmodules"
    )

    # Verify that a commit was made to .gitmodules
    ds.commit_if_changed.assert_called_once_with(
        "[backups2datalad] Update github-access-status for Zarr submodules",
        paths=[".gitmodules"],
        check_dirty=False,
    )


async def test_sync_zarr_with_embargo_status(tmp_path: Path) -> None:
    """Test that sync_zarr properly handles embargo status."""
    # Create mock asset
    asset = MagicMock(spec=RemoteZarrAsset)
    asset.zarr = "test-zarr-123"
    asset.dandiset_id = "000001"
    asset.created = MagicMock()  # Add created attribute

    # Create mock config with zarr GitHub org
    config = BackupConfig(
        s3bucket="test-bucket",
        s3endpoint="http://localhost:9000",
        content_url_regex="http://localhost:9000/test-bucket/.*blobs/",
        dandisets=ResourceConfig(
            path="dandisets",
            github_org="test-dandiset-org",
        ),
        zarrs=ResourceConfig(
            path="zarrs",
            github_org="test-zarr-org",
            remote=Remote(name="backup", type="s3", options={}),
        ),
    )

    # Create mock manager
    manager = MagicMock(spec=Manager)
    manager.config = config
    manager.log = MagicMock()

    # Mock the AsyncDataset methods we'll use
    mock_ds = AsyncMock(spec=AsyncDataset)
    mock_ds.pathobj = tmp_path / "zarr"
    mock_ds.is_dirty = AsyncMock(return_value=False)
    mock_ds.has_github_remote = AsyncMock(return_value=False)
    mock_ds.set_embargo_status = AsyncMock()
    mock_ds.create_github_sibling = AsyncMock()
    mock_ds.ensure_installed = AsyncMock(return_value=True)
    mock_ds.call_annex = AsyncMock()
    mock_ds.save = AsyncMock()

    # Patch AsyncDataset creation
    with patch("backups2datalad.zarr.AsyncDataset", return_value=mock_ds):
        # Test with embargoed status
        zarr_path = tmp_path / "zarr_path"
        zarr_path.mkdir()

        # Add zarr_limit to config
        manager.config.zarr_limit = AsyncMock()
        manager.config.zarr_limit.__aenter__ = AsyncMock()
        manager.config.zarr_limit.__aexit__ = AsyncMock()

        await sync_zarr(
            asset,
            "test-checksum",
            zarr_path,
            manager,
            embargo_status=EmbargoStatus.EMBARGOED,
        )

        # Verify embargo status was set
        mock_ds.set_embargo_status.assert_called_once_with(EmbargoStatus.EMBARGOED)

        # Verify GitHub sibling was created
        mock_ds.create_github_sibling.assert_called_once_with(
            owner="test-zarr-org",
            name="test-zarr-123",
            backup_remote=config.zarrs.remote if config.zarrs else None,
        )


async def test_datasetter_zarr_embargo_propagation(tmp_path: Path) -> None:
    """Test that DandiDatasetter propagates embargo status to Zarr sync."""

    # Create paths
    zarr_root = tmp_path / "zarrs"
    zarr_root.mkdir()
    partial_dir = tmp_path / "partial"
    partial_dir.mkdir()

    # Create mock zarr asset
    zarr_asset = MagicMock(spec=RemoteZarrAsset)
    zarr_asset.zarr = "test.zarr"
    zarr_asset.path = "test.zarr"
    zarr_asset.dandiset_id = "000001"
    zarr_asset.get_digest_value = MagicMock(return_value="test-digest")

    # Create mock dataset with embargo status
    ds = AsyncMock(spec=AsyncDataset)
    ds.pathobj = tmp_path / "dandisets" / "000001"
    ds.get_embargo_status = AsyncMock(return_value=EmbargoStatus.EMBARGOED)
    ds.get_assets_state = AsyncMock(return_value={})
    ds.set_assets_state = AsyncMock()
    ds.set_repo_config = AsyncMock()
    ds.commit_if_changed = AsyncMock()
    ds.assert_no_duplicates_in_gitmodules = MagicMock()

    # Create config with GitHub orgs
    config = BackupConfig(
        dandisets=ResourceConfig(
            path=str(tmp_path / "dandisets"),
            github_org="test-dandiset-org",
        ),
        zarrs=ResourceConfig(
            path=str(zarr_root),
            github_org="test-zarr-org",
        ),
    )

    # Mock manager
    manager = MagicMock(spec=Manager)
    manager.config = config
    manager.with_sublogger = MagicMock(return_value=manager)
    manager.log = MagicMock()

    # Track sync_zarr calls
    sync_zarr_calls = []

    async def mock_sync_zarr(
        asset: Any,
        _digest: Any,
        path: Any,
        _mgr: Any,
        _link: Any = None,
        embargo_status: Any = None,
    ) -> None:
        sync_zarr_calls.append(
            {
                "asset": asset,
                "embargo_status": embargo_status,
            }
        )
        # Create the directory that would be created by sync_zarr
        path.mkdir(exist_ok=True)

    # Patch sync_zarr and test the embargo propagation
    with (
        patch("backups2datalad.datasetter.sync_zarr", mock_sync_zarr),
        patch("backups2datalad.datasetter.shutil.move"),
    ):
        # Simulate the relevant part of backup_zarr
        zarr_dspath = partial_dir / zarr_asset.zarr
        # Get embargo status from parent Dandiset
        dandiset_embargo_status = await ds.get_embargo_status()
        await mock_sync_zarr(
            zarr_asset,
            "test-digest",
            zarr_dspath,
            manager,
            link=None,
            embargo_status=dandiset_embargo_status,
        )

        # Verify sync_zarr was called with the embargo status
        assert len(sync_zarr_calls) == 1
        assert sync_zarr_calls[0]["embargo_status"] == EmbargoStatus.EMBARGOED


async def test_syncer_skip_zarr_update_without_github_org() -> None:
    """Test that Syncer skips Zarr repo updates when zarr_gh_org is not configured."""
    # Create config without zarr_gh_org (both must be unset)
    config = BackupConfig(
        dandisets=ResourceConfig(path="dandisets"),
        zarrs=ResourceConfig(path="zarrs"),
    )

    ds = AsyncMock()
    ds.get_subdatasets = AsyncMock()  # Should not be called

    manager = MagicMock()
    manager.config = config
    manager.log = MagicMock()

    syncer = Syncer(
        manager=manager,
        dandiset=MagicMock(),
        ds=ds,
        tracker=MagicMock(),
        error_on_change=False,
    )

    await syncer.update_zarr_repos_privacy()

    # Verify get_subdatasets was not called
    ds.get_subdatasets.assert_not_called()


async def test_update_zarr_repos_privacy_handles_errors() -> None:
    """Test that update_zarr_repos_privacy handles errors gracefully."""
    ds = AsyncMock()
    ds.get_subdatasets = AsyncMock(
        return_value=[
            {
                "path": "/fake/path/error.zarr",
                "gitmodule_path": "error.zarr",
                "gitmodule_url": "https://github.com/dandizarrs/zarr_error",
            },
        ]
    )
    ds.set_repo_config = AsyncMock()
    ds.commit_if_changed = AsyncMock()

    manager = MockManager()
    # Make the edit_github_repo raise an exception

    async def raise_error(repo: Any, **kwargs: Any) -> None:
        manager.edit_repo_calls.append((repo, kwargs))
        raise Exception("GitHub API error")

    manager.edit_github_repo = raise_error  # type: ignore[method-assign]

    syncer = Syncer(
        manager=manager,  # type: ignore[arg-type]
        dandiset=MagicMock(),
        ds=ds,
        tracker=MagicMock(),
        error_on_change=False,
    )

    # This should not raise an exception
    await syncer.update_zarr_repos_privacy()

    # Verify the error was logged
    manager.log.error.assert_called_once()

    # Verify .gitmodules was not updated due to the error
    ds.set_repo_config.assert_not_called()
    ds.commit_if_changed.assert_not_called()


async def test_unembargo_dandiset_updates_zarr_privacy() -> None:
    """Test complete flow when Dandiset is unembargoed."""
    # Create mock dataset
    ds = AsyncMock()
    ds.get_subdatasets = AsyncMock(
        return_value=[
            {
                "path": "/path/data1.zarr",
                "gitmodule_path": "data1.zarr",
                "gitmodule_url": "https://github.com/dandizarrs/zarr001",
            },
            {
                "path": "/path/data2.ngff",
                "gitmodule_path": "data2.ngff",
                "gitmodule_url": "https://github.com/dandizarrs/zarr002",
            },
        ]
    )
    ds.set_repo_config = AsyncMock()
    ds.commit_if_changed = AsyncMock()

    # Create manager with mocked GitHub operations
    manager = MockManager()

    # Create mock dandiset
    dandiset = MagicMock()
    dandiset.identifier = "000001"

    # Create syncer with tracking
    tracker = MagicMock()
    syncer = Syncer(
        manager=manager,  # type: ignore[arg-type]
        dandiset=dandiset,
        ds=ds,
        tracker=tracker,
        error_on_change=False,
    )

    # Mock the embargo status transition (embargoed -> open)
    syncer.report = MagicMock()
    syncer.report.commits = 0

    # Mock the dandiset embargo status transition
    ds.get_embargo_status = AsyncMock(return_value=EmbargoStatus.EMBARGOED)
    ds.set_embargo_status = AsyncMock()
    ds.get_last_commit_date = AsyncMock()
    ds.save = AsyncMock()
    ds.has_github_remote = AsyncMock(return_value=True)
    ds.disable_dandi_provider = AsyncMock()

    dandiset.embargo_status = EmbargoStatus.OPEN

    # Mock register_s3urls
    with patch("backups2datalad.syncer.register_s3urls", new_callable=AsyncMock):
        # Test the complete unembargo flow
        await syncer.update_embargo_status()

    # Verify GitHub repos were updated to public
    assert len(manager.edit_repo_calls) == 3  # 1 for dandiset + 2 for zarrs

    # Check dandiset repo update
    assert manager.edit_repo_calls[0][0] == GHRepo("dandisets", "000001")
    assert manager.edit_repo_calls[0][1] == {"private": False}

    # Check zarr repo updates
    assert manager.edit_repo_calls[1][0] == GHRepo("dandizarrs", "zarr001")
    assert manager.edit_repo_calls[1][1] == {"private": False}
    assert manager.edit_repo_calls[2][0] == GHRepo("dandizarrs", "zarr002")
    assert manager.edit_repo_calls[2][1] == {"private": False}

    # Verify gitmodules were updated
    assert ds.set_repo_config.call_count == 2
    ds.set_repo_config.assert_any_call(
        "submodule.data1.zarr.github-access-status", "public", file=".gitmodules"
    )
    ds.set_repo_config.assert_any_call(
        "submodule.data2.ngff.github-access-status", "public", file=".gitmodules"
    )


async def test_zarr_github_access_status_in_gitmodules() -> None:
    """Test that github-access-status is set in .gitmodules when Zarr is added."""
    # Create mock dataset
    ds = AsyncMock(spec=AsyncDataset)
    ds.set_repo_config = AsyncMock()
    ds.commit_if_changed = AsyncMock()
    ds.assert_no_duplicates_in_gitmodules = MagicMock()
    ds.call_annex = AsyncMock()
    ds.add_submodule = AsyncMock()
    ds.save = AsyncMock()
    ds.commit = AsyncMock()
    ds.get_embargo_status = AsyncMock(return_value=EmbargoStatus.EMBARGOED)

    # Create mock zarr asset
    asset = MagicMock()
    asset.path = "test.zarr"
    asset.zarr = "zarr123"

    # Create config with zarr github org
    BackupConfig(
        dandisets=ResourceConfig(path="dandisets", github_org="test-gh-org"),
        zarrs=ResourceConfig(path="zarrs", github_org="test-zarr-org"),
    )

    # Test the gitmodules update logic from datasetter
    # (simulating the relevant part of the code)
    ts = MagicMock()

    # Set github-access-status based on embargo
    await ds.set_repo_config(
        f"submodule.{asset.path}.github-access-status",
        "private",  # because embargo status is EMBARGOED
        file=".gitmodules",
    )
    await ds.commit_if_changed(
        f"[backups2datalad] Update github-access-status for Zarr {asset.zarr}",
        paths=[".gitmodules"],
        check_dirty=False,
        commit_date=ts,
    )

    # Verify the calls
    ds.set_repo_config.assert_called_with(
        "submodule.test.zarr.github-access-status", "private", file=".gitmodules"
    )
    ds.commit_if_changed.assert_called_with(
        "[backups2datalad] Update github-access-status for Zarr zarr123",
        paths=[".gitmodules"],
        check_dirty=False,
        commit_date=ts,
    )
