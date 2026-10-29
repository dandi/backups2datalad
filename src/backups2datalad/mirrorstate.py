"""
Per-Dandiset state recorded in the superdataset's ``.gitmodules``, and the
planner that decides from it, without any mirror installed, which Dandisets a
run has to visit.  See "CI Mirroring" in CLAUDE.md.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
import json
import re
from typing import Any

from dandi.consts import DANDISET_ID_REGEX, EmbargoStatus

from .util import quiescence_wait

#: The committed ``.dandi/assets-state.json`` timestamp of the recorded commit
KEY_LAST_MODIFIED = "dandiset-last-modified"
#: Sorted, comma-separated versions tagged in the mirror
KEY_TAGGED_RELEASES = "dandiset-tagged-releases"
#: ``<commit>,<files>,<bytes>``, as `dandi.stats`; public Dandisets only
KEY_STATS = "dandiset-stats"
KEY_ACCESS = "github-access-status"

#: The keys `mirror_state_keys()` manages, which it may also remove
STATE_KEYS = (KEY_LAST_MODIFIED, KEY_TAGGED_RELEASES, KEY_STATS)


def parse_gitmodules_config(listing: str) -> dict[str, dict[str, str]]:
    """
    Group the output of ``git config -z --list`` of a ``.gitmodules`` file by
    submodule name.  Names may contain dots (Zarr paths); keys cannot.
    """
    entries: dict[str, dict[str, str]] = {}
    for item in listing.split("\0"):
        if not item:
            continue
        var, _, value = item.partition("\n")
        if not var.startswith("submodule."):
            continue
        name, _, key = var.removeprefix("submodule.").rpartition(".")
        entries.setdefault(name, {})[key] = value
    return entries


def mirror_state_keys(
    last_modified: datetime | None,
    tagged_releases: Iterable[str],
    stats: tuple[str, int, int] | None,
    embargoed: bool,
) -> dict[str, str | None]:
    """
    The `STATE_KEYS` values to record for a mirror; `None` means "remove".

    An embargoed Dandiset gets no stats: ``.gitmodules`` is public, and while
    its gitlink already shows *that* it changed, its size and file count would
    be news.
    """
    tags = sorted(tagged_releases)
    return {
        KEY_LAST_MODIFIED: (
            last_modified.isoformat() if last_modified is not None else None
        ),
        KEY_TAGGED_RELEASES: ",".join(tags) if tags else None,
        KEY_STATS: (None if embargoed or stats is None else ",".join(map(str, stats))),
    }


def stats_from_key(value: str | None, commit: str | None) -> tuple[int, int] | None:
    """``(files, size)`` from a `KEY_STATS` value, if it is for ``commit``"""
    if value is None or commit is None:
        return None
    try:
        stored_commit, files, size = value.split(",")
        if stored_commit == commit:
            return (int(files), int(size))
    except ValueError:
        pass
    return None


def tagged_releases_from_key(value: str | None) -> set[str] | None:
    if value is None:
        return None
    return {v for v in value.split(",") if v}


@dataclass
class MirrorResult:
    """
    The outcome of visiting one Dandiset mirror: what the superdataset has to
    record for it.  ``update-from-backup --results-dir`` writes these for
    ``record`` to apply.
    """

    identifier: str
    commit: str
    embargoed: bool
    keys: dict[str, str | None]
    #: Whether the visit committed anything
    changed: bool = False
    #: Needed to register a Dandiset new to the superdataset
    url: str | None = None
    datalad_id: str | None = None
    #: Subjects of the commits the superdataset does not record yet, newest
    #: first, for its commit message
    subjects: list[str] = field(default_factory=list)

    @property
    def access(self) -> str:
        return "private" if self.embargoed else "public"

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2) + "\n"

    @classmethod
    def from_json(cls, s: str) -> MirrorResult:
        return cls(**json.loads(s))


class Reason(StrEnum):
    NEW = "new"
    NO_STATE = "no-state"
    MODIFIED = "modified"
    UNTAGGED = "untagged-release"
    EMBARGO = "embargo-status"
    FORCED = "forced"


class EmbargoedPolicy(StrEnum):
    #: Plan public Dandisets only, and only count the embargoed ones, which is
    #: what a workflow in a public repository must do
    SKIP = "skip"
    INCLUDE = "include"
    ONLY = "only"


@dataclass
class RemoteInfo:
    """What `plan_updates()` needs to know of a Dandiset on the archive"""

    identifier: str
    modified: datetime
    embargo_status: EmbargoStatus
    size: int
    latest_published: str | None = None

    @classmethod
    def from_listing(
        cls, identifier: str, data: Mapping[str, Any], modified: datetime, size: int
    ) -> RemoteInfo:
        mrpv = data.get("most_recent_published_version")
        return cls(
            identifier=identifier,
            modified=modified,
            embargo_status=EmbargoStatus(data["embargo_status"]),
            size=size,
            latest_published=mrpv["version"] if mrpv else None,
        )

    @property
    def embargoed(self) -> bool:
        return self.embargo_status is not EmbargoStatus.OPEN


@dataclass
class PlannedDandiset:
    identifier: str
    reason: Reason
    size: int
    embargoed: bool
    #: When the recorded backup is from; `None` if there is none
    last_modified: datetime | None

    def as_json(self) -> dict[str, Any]:
        return {
            "id": self.identifier,
            "reason": str(self.reason),
            "size": self.size,
            "embargoed": self.embargoed,
        }


@dataclass
class Plan:
    #: The Dandisets to visit this run, in the order to visit them
    dandisets: list[PlannedDandiset] = field(default_factory=list)
    #: Stale, but left for a later run by ``max_dandisets``
    deferred: int = 0
    #: Embargoed Dandisets needing work that `EmbargoedPolicy.SKIP` left out
    embargoed_skipped: int = 0
    quiescent: int = 0
    unembargoing: int = 0
    #: Public mirrors of Dandisets no longer on the archive
    deleted: list[str] = field(default_factory=list)

    def as_json(self) -> dict[str, Any]:
        return {
            "dandisets": [d.as_json() for d in self.dandisets],
            "deferred": self.deferred,
            "embargoed_skipped": self.embargoed_skipped,
            "quiescent": self.quiescent,
            "unembargoing": self.unembargoing,
            "deleted": self.deleted,
        }


def plan_updates(
    remote: Iterable[RemoteInfo],
    gitmodules: Mapping[str, Mapping[str, str]],
    *,
    exclude: re.Pattern[str] | None = None,
    quiescent_period: float = 0,
    now: datetime | None = None,
    force: bool = False,
    embargoed: EmbargoedPolicy = EmbargoedPolicy.SKIP,
    max_dandisets: int | None = None,
    enable_tags: bool = True,
) -> Plan:
    """
    Decide which Dandisets need a sync, by the tests `update_dandiset()`
    applies, but against the state recorded in ``.gitmodules`` rather than in
    the mirrors.

    Order: the Dandisets stale longest take up to a quarter of a capped batch,
    so big ones cannot starve; the rest goes smallest first, so small updates
    are not stuck behind big ones.
    """
    plan = Plan()
    candidates: list[PlannedDandiset] = []
    seen: set[str] = set()
    for r in remote:
        seen.add(r.identifier)
        if exclude is not None and exclude.search(r.identifier):
            continue
        if r.embargo_status is EmbargoStatus.UNEMBARGOING:
            plan.unembargoing += 1
            continue
        entry = gitmodules.get(r.identifier)
        last_modified: datetime | None = None
        reason: Reason | None
        if entry is None:
            reason = Reason.NEW
        else:
            if (lm := entry.get(KEY_LAST_MODIFIED)) is not None:
                last_modified = datetime.fromisoformat(lm)
            access = "private" if r.embargoed else "public"
            tagged = tagged_releases_from_key(entry.get(KEY_TAGGED_RELEASES))
            if last_modified is None:
                reason = Reason.NO_STATE
            elif last_modified < r.modified:
                reason = Reason.MODIFIED
            elif entry.get(KEY_ACCESS, "public") != access:
                reason = Reason.EMBARGO
            elif (
                enable_tags
                and r.latest_published is not None
                and (tagged is None or r.latest_published not in tagged)
            ):
                reason = Reason.UNTAGGED
            elif force:
                reason = Reason.FORCED
            else:
                reason = None
        if reason is None:
            continue
        if (
            reason is not Reason.FORCED
            and quiescent_period > 0
            and quiescence_wait(r.modified, quiescent_period, now) > 0
        ):
            plan.quiescent += 1
            continue
        if r.embargoed and embargoed is EmbargoedPolicy.SKIP:
            plan.embargoed_skipped += 1
            continue
        if not r.embargoed and embargoed is EmbargoedPolicy.ONLY:
            continue
        candidates.append(
            PlannedDandiset(
                identifier=r.identifier,
                reason=reason,
                size=r.size,
                embargoed=r.embargoed,
                last_modified=last_modified,
            )
        )
    by_size = sorted(candidates, key=lambda d: (d.size, d.identifier))
    if max_dandisets is not None and len(candidates) > max_dandisets:
        oldest = sorted(
            (d for d in candidates if d.last_modified is not None),
            key=lambda d: (d.last_modified, d.identifier),
        )[: max_dandisets // 4]
        chosen = {d.identifier for d in oldest}
        for d in by_size:
            if len(chosen) >= max_dandisets:
                break
            chosen.add(d.identifier)
        plan.dandisets = [d for d in by_size if d.identifier in chosen]
        plan.deferred = len(candidates) - len(plan.dandisets)
    else:
        plan.dandisets = by_size
    if embargoed is not EmbargoedPolicy.ONLY:
        plan.deleted = sorted(
            did
            for did, entry in gitmodules.items()
            if re.fullmatch(DANDISET_ID_REGEX, did)
            and did not in seen
            and (exclude is None or not exclude.search(did))
            and entry.get(KEY_ACCESS, "public") == "public"
        )
    return plan
