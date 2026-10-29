"""Local run helper: generate data, run the pipeline, reconcile, print a report."""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from decimal import Decimal

from . import generator, pipeline, reference

OUT = "data/out"


def latest_positions(path: str) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    with open(path) as f:
        for line in f:
            r = json.loads(line)
            cur = latest.get(r["contract_id"])
            if cur is None or r["version"] > cur["version"]:
                latest[r["contract_id"]] = r
    return latest


def reconcile(lines: list[str], out_dir: str) -> list[str]:
    """Return a list of differences between pipeline output and the reference fold."""
    pipe = latest_positions(f"{out_dir}/position_history.jsonl")
    ref = reference.fold(lines)
    diffs = []
    for cid in sorted(set(pipe) | set(ref)):
        if cid not in pipe or cid not in ref:
            diffs.append(f"{cid}: present in only one side")
            continue
        r, p = ref[cid], pipe[cid]
        for k, rv in (("status", r.status), ("holder", r.holder),
                      ("outstanding_principal", r.outstanding_principal),
                      ("arrears_amount", r.arrears_amount), ("version", r.version)):
            if str(p[k]) != str(rv):
                diffs.append(f"{cid}: {k} pipeline={p[k]} reference={rv}")
    return diffs


def main() -> int:
    os.makedirs(OUT, exist_ok=True)
    lines = generator.generate()
    with open("data/events.jsonl", "w") as f:
        f.write("\n".join(lines) + "\n")
    pipeline.run(["--input", "data/events.jsonl", "--output_dir", OUT, "--runner", "DirectRunner"])

    pos = latest_positions(f"{OUT}/position_history.jsonl")
    dead = [json.loads(x) for x in open(f"{OUT}/dead_letter.jsonl")]

    print(f"\nInput lines: {len(lines)}   contracts tracked: {len(pos)}   dead letters: {len(dead)}")
    print("Status mix:", dict(Counter(p["status"] for p in pos.values())))
    by_holder: dict[str, Decimal] = {}
    for p in pos.values():
        if p["status"] in ("ACTIVE", "ARREARS"):
            by_holder[p["holder"]] = by_holder.get(p["holder"], Decimal(0)) + Decimal(p["outstanding_principal"])
    print("\nOutstanding principal by holder (live contracts), CHF")
    for h, v in sorted(by_holder.items()):
        print(f"  {h:<14}{v:>14,.2f}")
    print("\nDead letters (sample):")
    for d in dead[:6]:
        print(f"  {str(d['contract_id']):<11}{d['reason'][:80]}")

    diffs = reconcile(lines, OUT)
    print(f"\nReconciliation vs independent reference fold: {len(diffs)} differences")
    for d in diffs[:10]:
        print("  ", d)
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main())
