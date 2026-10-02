#!/bin/bash
# Discard uncommitted state in the Zarr mirrors of one Dandiset.
#
# Lists the Dandiset's Zarr assets from the DANDI API, and for each
# corresponding mirror under the Zarrs root runs `git reset --hard` and
# `git clean -dfx`.  Only mirrors whose Zarr id the API reports for THIS
# Dandiset are touched; anything else under the Zarrs root is left alone.
#
# This throws work away irrecoverably.  It is the blunt counterpart of
# `backups2datalad update-from-backup --zarr-dirty=reset+clean`, for cleaning up
# out of band rather than on the way through a backup run.
#
# Note `git clean -dfx` does not remove an untracked directory that is itself a
# git repository (typically a Zarr clone that was never registered as a
# submodule), and `-ff`, which would, is deliberately not used -- such a clone
# may hold the only copy of its content.  Mirrors still dirty afterwards are
# reported at the end for manual inspection, and are the whole reason this
# script re-checks rather than assuming success.
#
# DO NOT run this while a backup run is touching the same Dandiset: there is no
# locking anywhere in the tool, so this would discard work that the running
# process has staged and is about to commit.  Run with -n first.
#
# It runs one `git status` per mirror, serially.  On a Dandiset with tens of
# thousands of Zarrs (001412 has ~23,600) that is hours, not seconds -- it has
# not hung.  Narrow the work or run it under screen.
#
# Usage:
#   tools/reset-clean-dandiset-zarrs.sh [-n] [-v] [-i INSTANCE] DANDISET ZARRS_ROOT
#
#   -n              dry run: report what is dirty, change nothing
#   -v              also print the dirty paths of each mirror, not just a count
#   -i INSTANCE     API base URL  [default: https://api.dandiarchive.org/api]
#
# Exits non-zero if any mirror was left dirty or could not be read.
#
# Example:
#   tools/reset-clean-dandiset-zarrs.sh -n 001412 /mnt/backup/dandi/dandizarrs
#   tools/reset-clean-dandiset-zarrs.sh    001412 /mnt/backup/dandi/dandizarrs
#
# For an embargoed Dandiset, export DANDI_API_KEY first.

set -euo pipefail
# Without this, `set -e` is NOT inherited by the $(...) subshell that pages the
# API below, so a failed request mid-pagination would yield a short Zarr list
# and this script would happily reset only part of the Dandiset and exit 0.
shopt -s inherit_errexit

API="https://api.dandiarchive.org/api"
DRY_RUN=0
VERBOSE=0
USAGE="usage: $0 [-n] [-v] [-i INSTANCE] DANDISET ZARRS_ROOT"

while getopts ":nvi:" opt; do
	case "$opt" in
		n) DRY_RUN=1 ;;
		v) VERBOSE=1 ;;
		i) API="${OPTARG%/}" ;;
		*) echo "$USAGE" >&2; exit 2 ;;
	esac
done
shift $((OPTIND - 1))

if [ $# -ne 2 ]; then
	echo "$USAGE" >&2
	exit 2
fi

DANDISET="$1"
ZARRS_ROOT="${2%/}"

for cmd in curl jq git; do
	command -v "$cmd" >/dev/null || { echo "$cmd is required" >&2; exit 1; }
done

[ -d "$ZARRS_ROOT" ] || { echo "no such directory: $ZARRS_ROOT" >&2; exit 1; }

AUTH=()
if [ -n "${DANDI_API_KEY:-}" ]; then
	AUTH=(-H "Authorization: token $DANDI_API_KEY")
fi

# Collect the Zarr ids this Dandiset's draft refers to, following pagination.
# `zarr` is set only on Zarr assets, so `select(.zarr)` drops blob assets.
echo "Listing Zarr assets of $DANDISET from $API ..." >&2
zarr_ids=$(
	url="$API/dandisets/$DANDISET/versions/draft/assets/?page_size=1000"
	while [ -n "$url" ] && [ "$url" != "null" ]; do
		page=$(curl -fsSL "${AUTH[@]}" "$url")
		printf '%s\n' "$page" | jq -r '.results[] | select(.zarr) | .zarr'
		url=$(printf '%s\n' "$page" | jq -r '.next')
	done | sort -u
)

if [ -z "$zarr_ids" ]; then
	echo "No Zarr assets found for $DANDISET" >&2
	exit 0
fi

total=$(printf '%s\n' "$zarr_ids" | wc -l)
echo "$DANDISET refers to $total Zarr(s)" >&2

missing=0 clean=0 dirty=0 cleaned=0
residual=()
unreadable=()

# One `git status` per mirror, serially: on a Dandiset with tens of thousands of
# Zarrs this takes hours, not seconds.  See the header.
while IFS= read -r zid; do
	[ -n "$zid" ] || continue
	repo="$ZARRS_ROOT/$zid"
	if [ ! -e "$repo/.git" ]; then
		missing=$((missing + 1))
		continue
	fi
	# --ignore-submodules=none and -unormal are what backups2datalad's own
	# dirtiness check pins, so this agrees with what a backup run would say.
	# An unreadable repo must not abort the sweep over all the others.
	if ! status=$(git -C "$repo" status --porcelain \
			--untracked-files=normal --ignore-submodules=none 2>&1); then
		echo "ERROR  $zid: cannot read: ${status//$'\n'/ }" >&2
		unreadable+=("$zid")
		continue
	fi
	if [ -z "$status" ]; then
		clean=$((clean + 1))
		continue
	fi
	dirty=$((dirty + 1))
	n=$(printf '%s\n' "$status" | wc -l)
	if [ "$DRY_RUN" -eq 1 ]; then
		echo "DIRTY  $zid ($n path(s))"
		[ "$VERBOSE" -eq 1 ] && printf '%s\n' "$status" | sed 's/^/           /'
		continue
	fi
	echo "RESET  $zid ($n path(s)); discarding"
	[ "$VERBOSE" -eq 1 ] && printf '%s\n' "$status" | sed 's/^/           /'
	git -C "$repo" reset --hard --quiet
	git -C "$repo" clean -dfxq
	after=$(git -C "$repo" status --porcelain \
		--untracked-files=normal --ignore-submodules=none)
	if [ -n "$after" ]; then
		echo "       STILL DIRTY after reset+clean; needs manual inspection:"
		printf '%s\n' "$after" | sed 's/^/           /'
		residual+=("$zid")
	else
		cleaned=$((cleaned + 1))
	fi
done <<< "$zarr_ids"

echo >&2
if [ "$DRY_RUN" -eq 1 ]; then
	echo "Dry run: $total Zarr(s): $dirty dirty, $clean clean," \
		"$missing not mirrored, ${#unreadable[@]} unreadable" >&2
else
	echo "$total Zarr(s): $cleaned cleaned, $clean already clean," \
		"$missing not mirrored, ${#unreadable[@]} unreadable," \
		"${#residual[@]} still dirty" >&2
fi

if [ ${#residual[@]} -gt 0 ]; then
	echo "Still dirty (manual inspection needed):" >&2
	printf '  %s\n' "${residual[@]}" >&2
fi
if [ ${#unreadable[@]} -gt 0 ]; then
	echo "Unreadable (manual inspection needed):" >&2
	printf '  %s\n' "${unreadable[@]}" >&2
fi
if [ ${#residual[@]} -gt 0 ] || [ ${#unreadable[@]} -gt 0 ]; then
	exit 1
fi
