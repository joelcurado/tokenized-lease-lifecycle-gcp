"""Pure, framework-free state machine for one tokenized lease contract.

The same code runs inside the Beam DoFn and inside the reference
reconciliation, so there is exactly one definition of the business rules.

Events arrive from a sequenced source (for example a ledger or an
operational system) with a per-contract, gap-free `seq` starting at 1. The
machine applies events strictly in `seq` order:

  * seq lower than expected  -> duplicate delivery, ignored (idempotent)
  * seq higher than expected -> buffered until the gap is filled
  * domain rule violation    -> event is rejected with a reason (dead letter)
                                 and its seq is consumed so the stream moves on

Event types
  ISSUE              mint the lease token, fixes terms and first holder
  PAYMENT_RECEIVED   period paid in full
  PAYMENT_MISSED     period not paid on its due date
  TRANSFER           token moves from one holder to another
  EARLY_TERMINATION  contract ended before maturity
  MATURITY           all periods paid, residual settled, token redeemed
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from .schedule import Terms, build_schedule, outstanding_after, q

ISSUE = "ISSUE"
PAYMENT_RECEIVED = "PAYMENT_RECEIVED"
PAYMENT_MISSED = "PAYMENT_MISSED"
TRANSFER = "TRANSFER"
EARLY_TERMINATION = "EARLY_TERMINATION"
MATURITY = "MATURITY"
EVENT_TYPES = {ISSUE, PAYMENT_RECEIVED, PAYMENT_MISSED, TRANSFER, EARLY_TERMINATION, MATURITY}

ACTIVE, ARREARS, TERMINATED, MATURED = "ACTIVE", "ARREARS", "TERMINATED", "MATURED"
TERMINAL = {TERMINATED, MATURED}

MAX_BUFFER = 1000  # bound on out-of-order events held per contract


class Rejected(Exception):
    """Event violates a business rule."""


@dataclass
class ContractState:
    contract_id: str
    token_id: str
    holder: str
    currency: str
    terms: dict
    status: str = ACTIVE
    next_seq: int = 2  # ISSUE (seq 1) has just been applied when this exists
    version: int = 1
    paid: list[int] = field(default_factory=list)
    missed: list[int] = field(default_factory=list)
    pending: dict[str, dict] = field(default_factory=dict)  # seq (as str) -> event
    last_event_id: Optional[str] = None
    updated_at: Optional[str] = None
    outstanding_principal: str = "0.00"
    arrears_amount: str = "0.00"

    def to_json(self) -> str:
        return json.dumps(self.__dict__, sort_keys=True)

    @staticmethod
    def from_json(s: str) -> "ContractState":
        return ContractState(**json.loads(s))


def _refresh(st: ContractState) -> None:
    terms = Terms.from_dict(st.terms)
    sched = build_schedule(terms)
    by_no = {p.number: p for p in sched}
    paid = set(st.paid)
    st.outstanding_principal = str(outstanding_after(sched, paid, terms.principal))
    st.arrears_amount = str(q(sum((by_no[m].payment for m in st.missed), Decimal(0))))
    if st.status not in TERMINAL:
        st.status = ARREARS if st.missed else ACTIVE


def _issue(ev: dict) -> ContractState:
    p = ev["payload"]
    Terms.from_dict(p["terms"])  # validates
    st = ContractState(
        contract_id=ev["contract_id"],
        token_id=p["token_id"],
        holder=p["holder"],
        currency=p["currency"],
        terms=json.loads(json.dumps(p["terms"], default=str)),
        last_event_id=ev["event_id"],
        updated_at=ev["ts"],
    )
    _refresh(st)
    return st


def _apply(st: ContractState, ev: dict) -> None:
    t, p = ev["type"], ev.get("payload", {})
    terms = Terms.from_dict(st.terms)
    sched = {x.number: x for x in build_schedule(terms)}
    if st.status in TERMINAL:
        raise Rejected(f"contract is {st.status}, no further events allowed")

    if t == PAYMENT_RECEIVED:
        n = int(p["period"])
        if n not in sched:
            raise Rejected(f"period {n} outside schedule")
        if n in st.paid:
            raise Rejected(f"period {n} already paid")
        if q(Decimal(str(p["amount"]))) != sched[n].payment:
            raise Rejected(f"amount {p['amount']} != scheduled {sched[n].payment}")
        st.paid.append(n)
        if n in st.missed:
            st.missed.remove(n)
    elif t == PAYMENT_MISSED:
        n = int(p["period"])
        if n not in sched:
            raise Rejected(f"period {n} outside schedule")
        if n in st.paid or n in st.missed:
            raise Rejected(f"period {n} already settled or flagged")
        st.missed.append(n)
    elif t == TRANSFER:
        if p["from_holder"] != st.holder:
            raise Rejected(f"from_holder {p['from_holder']} is not current holder {st.holder}")
        if p["to_holder"] == st.holder:
            raise Rejected("transfer to self")
        st.holder = p["to_holder"]
    elif t == EARLY_TERMINATION:
        st.status = TERMINATED
    elif t == MATURITY:
        if len(st.paid) != terms.term_months:
            raise Rejected(f"maturity with {terms.term_months - len(st.paid)} unpaid periods")
        st.status = MATURED
    elif t == ISSUE:
        raise Rejected("contract already issued")
    else:
        raise Rejected(f"unknown event type {t}")


def validate_envelope(ev: dict) -> None:
    for k in ("event_id", "contract_id", "seq", "type", "ts"):
        if k not in ev:
            raise Rejected(f"missing field {k}")
    if ev["type"] not in EVENT_TYPES:
        raise Rejected(f"unknown event type {ev['type']}")
    if not isinstance(ev["seq"], int) or ev["seq"] < 1:
        raise Rejected("seq must be a positive integer")


def step(state: Optional[ContractState], ev: dict):
    """Apply one incoming event to the state of its contract.

    Returns (new_state, applied, rejected) where
      applied  = list of events applied (in order), each with the resulting state snapshot
      rejected = list of (event, reason)
    """
    applied: list[tuple[dict, ContractState]] = []
    rejected: list[tuple[dict, str]] = []

    try:
        validate_envelope(ev)
    except Rejected as e:
        return state, applied, [(ev, str(e))]

    seq = ev["seq"]

    # Before ISSUE: only seq 1 creates the contract, everything else waits in a side buffer.
    if state is None:
        if seq == 1 and ev["type"] == ISSUE:
            try:
                state = _issue(ev)
            except (Rejected, KeyError, ArithmeticError, ValueError) as e:
                return None, applied, [(ev, f"invalid ISSUE: {e}")]
            applied.append((ev, ContractState.from_json(state.to_json())))
        else:
            # Hold in a placeholder state until ISSUE arrives.
            state = ContractState(
                contract_id=ev["contract_id"], token_id="", holder="", currency="",
                terms={}, status="PENDING_ISSUE", next_seq=1, version=0,
            )
            if len(state.pending) < MAX_BUFFER:
                state.pending[str(seq)] = ev
            return state, applied, rejected
    elif state.status == "PENDING_ISSUE":
        if seq == 1 and ev["type"] == ISSUE:
            held = state.pending
            try:
                state = _issue(ev)
            except (Rejected, KeyError, ArithmeticError, ValueError) as e:
                return state, applied, [(ev, f"invalid ISSUE: {e}")]
            state.pending = held
            applied.append((ev, ContractState.from_json(state.to_json())))
        else:
            if len(state.pending) < MAX_BUFFER:
                state.pending[str(seq)] = ev
            return state, applied, rejected
    else:
        if seq < state.next_seq:
            return state, applied, rejected  # duplicate delivery, idempotent no-op
        if len(state.pending) >= MAX_BUFFER and seq != state.next_seq:
            return state, applied, [(ev, "reorder buffer full")]
        state.pending[str(seq)] = ev

    # Drain every buffered event that is now next in sequence.
    while str(state.next_seq) in state.pending:
        nxt = state.pending.pop(str(state.next_seq))
        state.next_seq += 1
        try:
            _apply(state, nxt)
        except (Rejected, KeyError, ArithmeticError, ValueError) as e:
            rejected.append((nxt, f"{type(e).__name__}: {e}"))
            continue
        state.version += 1
        state.last_event_id = nxt["event_id"]
        state.updated_at = nxt["ts"]
        _refresh(state)
        applied.append((nxt, ContractState.from_json(state.to_json())))
    return state, applied, rejected


def position_row(st: ContractState) -> dict:
    """Flat record written to BigQuery position_history."""
    return {
        "contract_id": st.contract_id,
        "token_id": st.token_id,
        "holder": st.holder,
        "currency": st.currency,
        "status": st.status,
        "outstanding_principal": st.outstanding_principal,
        "arrears_amount": st.arrears_amount,
        "periods_paid": len(st.paid),
        "periods_missed": len(st.missed),
        "term_months": int(st.terms["term_months"]),
        "version": st.version,
        "last_event_id": st.last_event_id,
        "updated_at": st.updated_at,
    }
