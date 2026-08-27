"""
DataLad procedures shipped with backups2datalad.

The procedures in this directory are made discoverable by `datalad` by pointing
the ``datalad.locations.extra-procedures`` option at this directory; see
`backups2datalad.util.custom_commit_env()`.
"""

from __future__ import annotations

from pathlib import Path

#: Directory containing the procedures, for `datalad.locations.extra-procedures`
PROCEDURES_PATH = Path(__file__).parent

#: Name (sans ``cfg_`` prefix) of the procedure that configures a Dandiset
#: mirror's ``annex.largefiles`` policy, as passed to ``datalad create -c``
DANDI_TEXT2GIT = "dandi_text2git"

#: Name of the same procedure as passed to ``datalad run-procedure``
DANDI_TEXT2GIT_PROCEDURE = f"cfg_{DANDI_TEXT2GIT}"
