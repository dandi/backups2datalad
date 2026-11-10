#!/bin/bash
# Compare CI's result for one Dandiset mirror with what drogon pushed:
#
#   mirror_dryrun_compare.sh REPO CI_REV
#
# REPO is a clone of the mirror that has CI_REV and a `github` remote.  Prints
# a JSON object: drogon's latest commit (drogon_*), and -- looked up in
# drogon's history by the state recorded in .dandi/assets-state.json -- its
# commit at the same archive state as CI's (match_*), if drogon has pushed one.
# The diff is against that commit if there is one, else against the latest.
#
# A busy Dandiset (e.g. 001697) changes between any two runs, so drogon's
# latest is rarely at CI's state; its commit at that state is the like-for-like
# comparison.  It may be pushed only after this ran: re-run the report job
# (which runs this again) to look again.
set -u
repo=$1
ci=$2
# How far back to look: drogon's syncs of a Dandiset, newest first
MAX_HISTORY=${MAX_HISTORY:-200}

state() {
    git -C "$repo" show "$1:.dandi/assets-state.json" 2>/dev/null | jq -r '.timestamp // empty'
}

ci_tree=$(git -C "$repo" rev-parse "$ci^{tree}")
ci_state=$(state "$ci")
dr=""; dr_tree=""; dr_state=""; match=""; match_tree=""
target=""; shortstat=""; files=""; patch=""
if git -C "$repo" fetch -q github draft; then
    dr=$(git -C "$repo" rev-parse FETCH_HEAD)
    dr_tree=$(git -C "$repo" rev-parse "$dr^{tree}")
    dr_state=$(state "$dr")
    if [ -n "$ci_state" ]; then
        # The commits that set a state, newest first.  States only grow along
        # the history (ISO 8601 UTC, same format), so stop at the first one
        # older than CI's.  Later commits may keep a state (metadata-only
        # updates), so the commit at that state is the last one before the
        # next state was set: the parent of `newer`, or drogon's latest
        newer=""
        while read -r c; do
            s=$(state "$c")
            if [ "$s" = "$ci_state" ]; then
                if [ -n "$newer" ]; then match=$(git -C "$repo" rev-parse "$newer^"); else match=$dr; fi
                break
            fi
            if [ -n "$s" ] && [[ "$s" < "$ci_state" ]]; then break; fi
            newer=$c
        done < <(git -C "$repo" log --first-parent --format=%H -n "$MAX_HISTORY" "$dr" -- .dandi/assets-state.json)
    fi
    if [ -n "$match" ]; then
        match_tree=$(git -C "$repo" rev-parse "$match^{tree}")
        target=$match
    else
        target=$dr
    fi
    shortstat=$(git -C "$repo" diff --shortstat "$target" "$ci")
    files=$(git -C "$repo" diff --name-status "$target" "$ci" | head -40)
    # The text of the differences (annexed files differ as links)
    patch=$(git -C "$repo" diff "$target" "$ci" | head -150)
fi
jq -n --arg ci "$ci" --arg ci_tree "$ci_tree" --arg ci_state "$ci_state" \
    --arg dr "$dr" --arg dr_tree "$dr_tree" --arg dr_state "$dr_state" \
    --arg match "$match" --arg match_tree "$match_tree" --arg target "$target" \
    --arg shortstat "$shortstat" --arg files "$files" --arg patch "$patch" \
    '{ci_commit: $ci, ci_tree: $ci_tree, ci_state: $ci_state,
      drogon_commit: $dr, drogon_tree: $dr_tree, drogon_state: $dr_state,
      match_commit: $match, match_tree: $match_tree, diff_against: $target,
      diff_shortstat: $shortstat,
      diff_files: ($files | split("\n") | map(select(. != ""))),
      diff_patch: $patch}'
