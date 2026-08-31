"""
DataLad configuration procedures shipped with backups2datalad.

The directory containing this package is passed to ``datalad`` as
``datalad.locations.extra-procedures`` so that ``datalad create -c dandiset``
finds `cfg_dandiset.py`.
"""

from __future__ import annotations

from pathlib import Path

#: Directory in which DataLad looks for our ``cfg_*`` procedures
PROCEDURES_DIR = Path(__file__).parent
