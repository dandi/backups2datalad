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

# How many consecutive rate-limited GitHub responses are slept out and
# retried; on the next one the gate gives up and further GitHub mutations in
# this process fail fast instead of sleeping.  With the fallback below that is
# up to ~5 h of cooldowns (60 s, 120 s, ... 1920 s, then 3600 s each).
GITHUB_RATE_LIMIT_ATTEMPTS = 10

# Minimum seconds between two mutating (POST/PATCH/...) GitHub requests
GITHUB_MUTATION_SPACING = 1.0

# Seconds to wait after a rate-limited response that carries no usable
# `retry-after` / `x-ratelimit-reset` header (doubled per consecutive hit, up
# to GITHUB_RATE_LIMIT_FALLBACK_MAX)
GITHUB_RATE_LIMIT_FALLBACK = 60

# Cap on the doubled fallback wait: an hour is the longest window GitHub
# documents for its secondary limits (500 content-creating requests per hour)
GITHUB_RATE_LIMIT_FALLBACK_MAX = 3600

# Seconds to allow DataLad's `create_sibling_github` (an untimed `requests`
# call in a worker thread) while holding the GitHub gate's lock
GITHUB_CREATE_TIMEOUT = 120

# Seconds to keep waiting, with the gate's lock released, for a
# `create_sibling_github` call that did not finish within
# GITHUB_CREATE_TIMEOUT.  It may well have created the repository (and go on
# to configure the sibling), so its outcome is used if it finishes in time;
# otherwise it is abandoned and the creation is retried, adopting the
# repository if the abandoned call created it (`existing="reconfigure"`).
GITHUB_CREATE_GRACE = 600

# How many times a creation abandoned after GITHUB_CREATE_GRACE is retried
# before the timeout is raised
GITHUB_CREATE_TIMEOUT_RETRIES = 2

# Seconds to wait before each retry of a GitHub repository creation that
# failed with a server error (5xx); one retry per entry, then the error is
# raised.  Such a failure may have created the repository after all, which
# the retry adopts (`existing="reconfigure"`).
GITHUB_SERVER_ERROR_WAITS = (10, 30, 90)
