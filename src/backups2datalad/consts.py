import platform

import httpx

from . import __url__

DEFAULT_BRANCH = "draft"

DEFAULT_GIT_ANNEX_JOBS = 10

DEFAULT_WORKERS = 5

# Maximum number of Zarrs to process at once
ZARR_LIMIT = 32

USER_AGENT = "backups2datalad ({}) httpx/{} {}/{}".format(
    __url__,
    httpx.__version__,
    platform.python_implementation(),
    platform.python_version(),
)

GIT_OPTIONS = ["-c", "receive.autogc=0", "-c", "gc.auto=0"]

# Maximum number of times to repeatedly sync a Zarr in case of local-vs.-server
# checksum mismatch
MAX_ZARR_SYNCS = 5
