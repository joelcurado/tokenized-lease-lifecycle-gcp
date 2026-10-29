"""Synthetic event generator. All data is invented; no client data is used.

Produces a realistic, messy stream for a small portfolio of lease tokens:
  * events are shuffled globally, so they arrive out of order
  * a few events are delivered twice
  * a few events break business rules (wrong-holder transfer, wrong amount)
  * a few lines are not valid JSON
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from .schedule import Terms, build_schedule

HOLDERS = ["SPV-ALPINE-1", "FUND-RHEIN", "BANK-LEMAN", "FUND-JURA", "SPV-ALPINE-2"]


def _ts(base: datetime, days: int) -> str:
    return (base + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate(n_contracts: int = 40, seed: int = 7) -> list[str]:
    rng = random.Random(seed)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    lines: list[str] = []
    for c in range(1, n_contracts + 1):
        cid = f"LEASE-{c:04d}"
        term = rng.choice([24, 36, 48])
        principal = Decimal(rng.randrange(18_000, 90_000, 500))
        residual = (principal * Decimal(rng.choice(["0.20", "0.30", "0.40"]))).quantize(Decimal("0.01"))
        terms = {
            "principal": str(principal),
            "annual_rate": rng.choice(["0.035", "0.042", "0.049"]),
            "term_months": term,
            "residual_value": str(residual),
        }
        sched = build_schedule(Terms.from_dict(terms))
        holder = rng.choice(HOLDERS)
        seq = 0
        day = 0

        def ev(typ: str, payload: dict, d: int) -> dict:
            nonlocal seq
            seq += 1
            return {
                "event_id": f"{cid}-E{seq:04d}", "contract_id": cid, "seq": seq,
                "type": typ, "ts": _ts(base, d), "payload": payload,
            }

        events = [ev("ISSUE", {"token_id": f"TKN-{c:04d}", "holder": holder, "currency": "CHF", "terms": terms}, 0)]
        horizon = rng.randrange(6, term + 1)  # how far this contract has progressed
        outcome = rng.choices(["running", "terminate", "mature"], weights=[6, 1, 2])[0]
        if outcome == "mature":
            horizon = term
        missed: list[int] = []
        for p in sched[:horizon]:
            day = 30 * p.number
            if rng.random() < 0.07 and outcome != "mature":
                missed.append(p.number)
                events.append(ev("PAYMENT_MISSED", {"period": p.number}, day))
            else:
                events.append(ev("PAYMENT_RECEIVED", {"period": p.number, "amount": str(p.payment)}, day))
            if rng.random() < 0.05:
                new = rng.choice([h for h in HOLDERS if h != holder])
                events.append(ev("TRANSFER", {"from_holder": holder, "to_holder": new}, day + 1))
                holder = new
        # a late cure for some missed periods
        for m in missed[:1]:
            events.append(ev("PAYMENT_RECEIVED", {"period": m, "amount": str(sched[m - 1].payment)}, 30 * m + 40))
        if outcome == "terminate":
            events.append(ev("EARLY_TERMINATION", {"reason": "customer request"}, day + 45))
        elif outcome == "mature":
            events.append(ev("MATURITY", {"residual_settled": str(residual)}, 30 * term + 5))

        # injected faults (these consume a seq so the stream stays gap-free)
        if c % 9 == 0 and outcome == "running":
            events.append(ev("TRANSFER", {"from_holder": "SOMEONE-ELSE", "to_holder": "FUND-JURA"}, day + 2))
        if c % 11 == 0 and outcome == "running":
            events.append(ev("PAYMENT_RECEIVED", {"period": horizon + 1, "amount": "1.00"}, day + 31))

        for e in events:
            lines.append(json.dumps(e))
            if rng.random() < 0.04:
                lines.append(json.dumps(e))  # duplicate delivery

    rng.shuffle(lines)
    for _ in range(3):
        lines.insert(rng.randrange(len(lines)), "{this is not valid json")
    return lines


if __name__ == "__main__":
    import sys

    out = sys.argv[1] if len(sys.argv) > 1 else "data/events.jsonl"
    data = generate()
    with open(out, "w") as f:
        f.write("\n".join(data) + "\n")
    print(f"wrote {len(data)} lines to {out}")
