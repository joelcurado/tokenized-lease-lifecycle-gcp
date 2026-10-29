import json
import random
from decimal import Decimal

from lease_demo import generator, ledger, reference
from lease_demo.schedule import Terms, build_schedule, monthly_payment, q

TERMS = {"principal": "30000", "annual_rate": "0.042", "term_months": 36, "residual_value": "9000.00"}


def issue(seq=1, holder="H1"):
    return {"event_id": f"E{seq}", "contract_id": "C1", "seq": seq, "type": "ISSUE", "ts": "2026-01-01T00:00:00Z",
            "payload": {"token_id": "T1", "holder": holder, "currency": "CHF", "terms": TERMS}}


def ev(seq, typ, payload):
    return {"event_id": f"E{seq}", "contract_id": "C1", "seq": seq, "type": typ,
            "ts": "2026-02-01T00:00:00Z", "payload": payload}


def pay(seq, period):
    amt = str(build_schedule(Terms.from_dict(TERMS))[period - 1].payment)
    return ev(seq, "PAYMENT_RECEIVED", {"period": period, "amount": amt})


def run(events):
    st, rej = None, []
    for e in events:
        st, _, r = ledger.step(st, e)
        rej += r
    return st, rej


# ---- schedule -------------------------------------------------------------
def test_schedule_amortizes_to_residual():
    t = Terms.from_dict(TERMS)
    s = build_schedule(t)
    assert len(s) == 36
    assert abs(s[-1].balance_after - t.residual_value) <= Decimal("0.50")  # cent rounding drift only
    assert all(p.payment == monthly_payment(t) for p in s)


def test_principal_plus_interest_equals_payment():
    for p in build_schedule(Terms.from_dict(TERMS)):
        assert p.interest + p.principal == p.payment


def test_zero_rate_lease():
    t = Terms.from_dict({**TERMS, "annual_rate": "0"})
    assert monthly_payment(t) == q((Decimal("30000") - Decimal("9000")) / 36)


# ---- state machine --------------------------------------------------------
def test_issue_creates_active_token():
    st, rej = run([issue()])
    assert st.status == "ACTIVE" and st.holder == "H1" and st.outstanding_principal == "30000.00" and not rej


def test_payment_reduces_principal_and_missed_flags_arrears_then_cure():
    st, _ = run([issue(), pay(2, 1), ev(3, "PAYMENT_MISSED", {"period": 2})])
    assert st.status == "ARREARS" and Decimal(st.arrears_amount) > 0
    st, rej = run([issue(), pay(2, 1), ev(3, "PAYMENT_MISSED", {"period": 2}), pay(4, 2)])
    assert st.status == "ACTIVE" and st.arrears_amount == "0.00" and not rej


def test_wrong_amount_is_rejected_and_state_unchanged():
    st, rej = run([issue(), ev(2, "PAYMENT_RECEIVED", {"period": 1, "amount": "1.00"})])
    assert len(rej) == 1 and st.paid == [] and st.version == 1


def test_transfer_requires_current_holder():
    st, rej = run([issue(), ev(2, "TRANSFER", {"from_holder": "NOT-ME", "to_holder": "H2"})])
    assert st.holder == "H1" and len(rej) == 1
    st, rej = run([issue(), ev(2, "TRANSFER", {"from_holder": "H1", "to_holder": "H2"})])
    assert st.holder == "H2" and not rej


def test_maturity_requires_all_periods_paid():
    st, rej = run([issue(), pay(2, 1), ev(3, "MATURITY", {})])
    assert st.status != "MATURED" and len(rej) == 1
    events = [issue()] + [pay(i + 1, i) for i in range(1, 37)] + [ev(38, "MATURITY", {})]
    st, rej = run(events)
    assert st.status == "MATURED" and not rej


def test_terminal_state_rejects_further_events():
    st, rej = run([issue(), ev(2, "EARLY_TERMINATION", {}), pay(3, 1)])
    assert st.status == "TERMINATED" and len(rej) == 1


def test_duplicates_are_idempotent():
    st1, _ = run([issue(), pay(2, 1)])
    st2, rej = run([issue(), pay(2, 1), pay(2, 1), issue(), pay(2, 1)])
    assert st1.to_json() == st2.to_json() and not rej


def test_out_of_order_delivery_converges_to_same_state():
    base = [issue()] + [pay(i + 1, i) for i in range(1, 13)] + [ev(14, "TRANSFER", {"from_holder": "H1", "to_holder": "H2"})]
    expected, _ = run(base)
    for seed in range(25):
        shuffled = base[:]
        random.Random(seed).shuffle(shuffled)
        got, rej = run(shuffled)
        assert got.version == expected.version and got.holder == "H2" and not rej
        assert got.outstanding_principal == expected.outstanding_principal


def test_events_before_issue_are_buffered():
    st, rej = run([pay(2, 1), pay(3, 2), issue()])
    assert st.version == 3 and len(st.paid) == 2 and not rej


def test_malformed_envelope_rejected():
    _, _, rej = ledger.step(None, {"event_id": "x"})
    assert rej and "missing field" in rej[0][1]


# ---- generator + reference ------------------------------------------------
def test_reference_fold_is_stable_under_reshuffle_and_duplicates():
    lines = generator.generate(n_contracts=15, seed=3)
    a = {k: v.to_json() for k, v in reference.fold(lines).items()}
    shuffled = lines[:] + lines[:50]
    random.Random(1).shuffle(shuffled)
    b = {k: v.to_json() for k, v in reference.fold(shuffled).items()}
    assert a == b
