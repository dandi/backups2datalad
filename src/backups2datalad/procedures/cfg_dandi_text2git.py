"""
Procedure to configure git-annex to add text files directly to Git, up to a
size limit.

This is a fixed-up version of DataLad's ``cfg_text2git`` procedure: text files
are added to Git as they are there, but files above
`backups2datalad.gitattributes.TEXT_SIZE_LIMIT` are added to git-annex even if
they are text, and the exemption for Git's own files is written *after* the
general rule so that it actually takes effect.

Run as::

    datalad run-procedure -d <dataset> cfg_dandi_text2git

or at dataset creation time as ``datalad create -c dandi_text2git <path>``.  In
either case ``datalad.locations.extra-procedures`` needs to point at the
directory containing this file.
"""

from __future__ import annotations

import sys

from datalad.distribution.dataset import require_dataset

from backups2datalad.gitattributes import apply_policy

ds = require_dataset(sys.argv[1], check_installed=True, purpose="configuration")

if apply_policy(ds.pathobj):
    ds.save(
        ds.pathobj / ".gitattributes",
        message="[backups2datalad] Configure annex.largefiles policy",
    )
