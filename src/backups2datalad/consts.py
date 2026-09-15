import platform

import httpx

from . import __url__

DEFAULT_BRANCH = "draft"

DEFAULT_GIT_ANNEX_JOBS = 10

DEFAULT_WORKERS = 5

# Number of seconds that must have elapsed since a Dandiset's "modified"
# timestamp before we consider it settled enough to back up.  A Dandiset that
# was touched more recently than this is likely still being changed (e.g., a
# mass upload or delete of assets is in progress), and backing it up while the
# server-side state is in flux leads to inconsistencies between the asset
# listing we paginate through and the assets we later query.
DEFAULT_QUIESCENT_PERIOD = 30.0

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

# GitHub rate-limit handling (see docs/github-zarr-rate-limits-plan.md).  These
# are the only locally chosen numbers; sleep durations come from GitHub's
# `retry-after` / `x-ratelimit-reset` headers or, failing those, from its
# documented fallback of "at least one minute", doubling.

# Consecutive rate-limited responses after which further GitHub mutations in
# this process fail fast instead of sleeping
GITHUB_RATE_LIMIT_ATTEMPTS = 5

# Minimum seconds between two mutating (POST/PATCH/...) GitHub requests
GITHUB_MUTATION_SPACING = 1.0

# Seconds to wait after a rate-limited response that carries no usable
# `retry-after` / `x-ratelimit-reset` header (doubled per consecutive hit)
GITHUB_RATE_LIMIT_FALLBACK = 60

# Seconds to allow DataLad's `create_sibling_github` (an untimed `requests`
# call in a worker thread) before giving up on the attempt
GITHUB_CREATE_TIMEOUT = 120
