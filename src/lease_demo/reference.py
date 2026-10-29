"""Independent reference fold used to reconcile the pipeline output.

It deliberately avoids Beam and avoids the buffering logic in ledger.step:
events are grouped per contract, sorted by seq, de-duplicated, and replayed
through the same business rules. If the streaming pipeline and this batch
fold ever disagree, the reconciliation check fails.
"""
from __future__ import annotations

import json
from collections import defaultdict

from . import ledger


def fold(lines: list[str]) -> dict[str, ledger.ContractState]:
    by_contract: dict[str, dict[int, dict]] = defaultdict(dict)
    for line in lines:
        try:
            ev = json.loads(line)
            ledger.validate_envelope(ev)
        except (ValueError, ledger.Rejected):
            continue
        by_contract[ev["contract_id"]].setdefault(ev["seq"], ev)

    out: dict[str, ledger.ContractState] = {}
    for cid, events in by_contract.items():
        if 1 not in events or events[1]["type"] != ledger.ISSUE:
            continue
        st = None
        for seq in sorted(events):
            st, _, _ = ledger.step(st, events[seq])
        out[cid] = st
    return out
