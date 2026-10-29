"""Cash-flow schedule for a tokenized lease contract.

The schedule is an annuity with a residual value (balloon) at maturity, which
is how a typical vehicle or equipment lease is structured. The approach is
inspired by the ACTUS idea of deriving every future cash-flow event
deterministically from contract terms. It is NOT an ACTUS-conformant
implementation.

All money is Decimal, quantized to 2 places per period, so that the schedule
is exactly reproducible and reconcilable.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, getcontext

getcontext().prec = 28
CENT = Decimal("0.01")


def q(x: Decimal) -> Decimal:
    return x.quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class Terms:
    principal: Decimal  # financed amount at issuance
    annual_rate: Decimal  # e.g. Decimal("0.045")
    term_months: int
    residual_value: Decimal  # balloon due at maturity

    @staticmethod
    def from_dict(d: dict) -> "Terms":
        return Terms(
            principal=Decimal(str(d["principal"])),
            annual_rate=Decimal(str(d["annual_rate"])),
            term_months=int(d["term_months"]),
            residual_value=Decimal(str(d["residual_value"])),
        )


@dataclass(frozen=True)
class Period:
    number: int  # 1..term_months
    payment: Decimal
    interest: Decimal
    principal: Decimal
    balance_after: Decimal  # outstanding principal if this period is paid


def monthly_payment(t: Terms) -> Decimal:
    i = t.annual_rate / Decimal(12)
    n = t.term_months
    if i == 0:
        return q((t.principal - t.residual_value) / Decimal(n))
    disc = (Decimal(1) + i) ** n
    pv_residual = t.residual_value / disc
    return q((t.principal - pv_residual) * i / (Decimal(1) - Decimal(1) / disc))


def build_schedule(t: Terms) -> list[Period]:
    i = t.annual_rate / Decimal(12)
    pmt = monthly_payment(t)
    bal = t.principal
    out: list[Period] = []
    for k in range(1, t.term_months + 1):
        interest = q(bal * i)
        princ = q(pmt - interest)
        bal = q(bal - princ)
        out.append(Period(k, pmt, interest, princ, bal))
    return out


def outstanding_after(schedule: list[Period], paid: set[int], principal: Decimal) -> Decimal:
    """Principal still owed given the set of paid period numbers."""
    return q(principal - sum((p.principal for p in schedule if p.number in paid), Decimal(0)))
