"""
Summarize a dry run of the CI mirroring workflow (mirror-dryrun.yml) as
Markdown: what `plan` chose, what each sync did starting from the commits the
superdataset records, and how that compares with what the cron on drogon has
pushed for the same Dandisets by the time of the comparison.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

MARKER = "<!-- backups2datalad:mirror-dryrun -->"


def load_dir(path: Path) -> dict[str, dict[str, Any]]:
    out = {}
    if path.is_dir():
        for p in sorted(path.glob("*.json")):
            out[p.stem] = json.loads(p.read_text())
    return out


def short(sha: str | None) -> str:
    return sha[:8] if sha else "–"


def vs_drogon(cmp: dict[str, Any]) -> str:
    if not cmp.get("ci_tree"):
        return "–"
    if not cmp.get("drogon_commit"):
        return "no mirror on GitHub"
    ci_state, dr_state = cmp.get("ci_state"), cmp.get("drogon_state")
    if cmp.get("ci_tree") == cmp.get("drogon_tree"):
        return "✅ same content"
    if ci_state and dr_state and dr_state < ci_state:
        return f"⏳ drogon not there yet (at {dr_state})"
    if ci_state and dr_state and dr_state > ci_state:
        return f"drogon newer ({dr_state})"
    return f"❌ differs: {cmp.get('diff_shortstat') or '?'}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", type=Path, required=True)
    ap.add_argument("--results", type=Path, required=True)
    ap.add_argument("--compare", type=Path, required=True)
    ap.add_argument("--superds-stat", type=Path)
    ap.add_argument("--header", default="")
    args = ap.parse_args()

    plan = json.loads(args.plan.read_text()) if args.plan.exists() else None
    results = load_dir(args.results)
    compare = load_dir(args.compare)

    lines = [MARKER, "## CI mirroring dry run", "", args.header, ""]
    if plan is None:
        lines.append("`plan` did not produce a plan; see the run's log.")
        print("\n".join(lines))
        return
    planned = plan["dandisets"]
    sources: dict[str, int] = {}
    for d in planned:
        key = f"{d['reason']} ({d['state_from'] or 'no state'})"
        sources[key] = sources.get(key, 0) + 1
    lines.append(
        f"**Plan:** {len(planned)} to sync"
        + (f" ({', '.join(f'{v} {k}' for k, v in sorted(sources.items()))})"
           if sources else "")
        + f"; {plan['deferred']} deferred, {plan['quiescent']} quiescent,"
        f" {plan['embargoed_skipped']} embargoed skipped,"
        f" {plan['unembargoing']} unembargoing;"
        f" {len(plan['deleted'])} no longer on the archive"
        + (f" ({', '.join(plan['deleted'])})" if plan["deleted"] else "")
        + "."
    )
    lines.append("")
    if planned:
        lines += [
            "| Dandiset | planned as | size | CI sync (from recorded commit) |"
            " vs. drogon's mirror now |",
            "| --- | --- | ---: | --- | --- |",
        ]
        details = []
        for d in planned:
            did = d["id"]
            r = results.get(did)
            c = compare.get(did, {})
            if r is None:
                ci = f"❌ {c.get('outcome', 'failed')}"
            elif r["changed"]:
                ci = (
                    f"{len(r['subjects'])} commit(s) →"
                    f" `{short(r['commit'])}`"
                )
            else:
                ci = f"no change (`{short(r['commit'])}`)"
            lines.append(
                f"| {did} | {d['reason']} | {d['size']:,} | {ci} |"
                f" {vs_drogon(c)} |"
            )
            if r is not None and r["subjects"]:
                details += [f"**{did}**, CI's new commits:", ""]
                details += [f"- {s}" for s in r["subjects"][:10]]
                details.append("")
            if c.get("diff_files"):
                details += [f"**{did}**, differences from drogon's mirror:", ""]
                details.append("```")
                details += c["diff_files"]
                details += ["```", ""]
        if details:
            lines += ["", "<details><summary>Details</summary>", ""]
            lines += details
            lines += ["", "</details>"]
    if args.superds_stat and args.superds_stat.exists():
        stat = args.superds_stat.read_text().strip()
        lines += [
            "",
            "<details><summary>What <code>record</code> would commit to the"
            " superdataset (not pushed)</summary>",
            "",
            "```",
            stat or "(nothing)",
            "```",
            "</details>",
        ]
    print("\n".join(lines))


if __name__ == "__main__":
    main()
