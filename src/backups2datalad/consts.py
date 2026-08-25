import platform

import httpx

from . import __url__

DEFAULT_BRANCH = "draft"

DEFAULT_GIT_ANNEX_JOBS = 10

DEFAULT_WORKERS = 5

MINIMUM_GIT_ANNEX_VERSION = "10.20240430"

# Maximum number of Zarrs to process at once
ZARR_LIMIT = 10

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

# Number of requests to feed to a `git annex --batch` process at a time.
# Requests within a chunk are pipelined: they are all written to the
# subprocess while its responses are read concurrently, instead of paying a
# send-wait-receive round trip per request.
BATCH_CHUNK_SIZE = 1000

# Number of git-annex-branch-modifying batch requests (i.e. `registerurl`)
# after which the batch process is closed & reopened.  git-annex only commits
# its journal to the git-annex branch when the process exits, and every
# journalled change is a separate file in the flat `.git/annex/journal/`
# directory, so a process that is kept alive for a Zarr with tens of thousands
# of entries ends up creating (and repeatedly looking up) files in a directory
# with a comparable number of entries, which degrades badly.  Restarting the
# process periodically bounds the journal size.
JOURNAL_FLUSH_INTERVAL = 5000
