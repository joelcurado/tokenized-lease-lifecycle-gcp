"""End to end: Beam DirectRunner output vs independent reference, then the SQL views on DuckDB."""
import json
from pathlib import Path

import pytest

from lease_demo import generator, pipeline, report

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def run_output(tmp_path_factory):
    d = tmp_path_factory.mktemp("run")
    lines = generator.generate(n_contracts=30, seed=11)
    (d / "events.jsonl").write_text("\n".join(lines) + "\n")
    pipeline.run(["--input", str(d / "events.jsonl"), "--output_dir", str(d), "--runner", "DirectRunner"])
    return d, lines


def test_pipeline_matches_reference_fold(run_output):
    d, lines = run_output
    assert report.reconcile(lines, str(d)) == []


def test_dead_letters_capture_bad_input(run_output):
    d, _ = run_output
    dead = [json.loads(x) for x in (d / "dead_letter.jsonl").read_text().splitlines()]
    assert sum(1 for x in dead if x["reason"].startswith("parse")) == 3
    assert any("current holder" in x["reason"] or "scheduled" in x["reason"] or "outside schedule" in x["reason"] for x in dead)


def test_every_applied_event_is_unique(run_output):
    d, _ = run_output
    ids = [json.loads(x)["event_id"] for x in (d / "applied_events.jsonl").read_text().splitlines()]
    assert len(ids) == len(set(ids))


duckdb = pytest.importorskip("duckdb")


def _sql_for_duckdb(text: str) -> str:
    return text.replace("`${DATASET}.", "").replace("`", "").replace("${DATASET}.", "")


def test_sql_views_and_control(run_output):
    d, _ = run_output
    con = duckdb.connect()
    con.execute("""CREATE TABLE position_history(contract_id VARCHAR, token_id VARCHAR, holder VARCHAR, currency VARCHAR,
        status VARCHAR, outstanding_principal DECIMAL(18,2), arrears_amount DECIMAL(18,2), periods_paid INTEGER,
        periods_missed INTEGER, term_months INTEGER, version INTEGER, last_event_id VARCHAR, updated_at VARCHAR)""")
    con.execute("CREATE TABLE applied_events(event_id VARCHAR, contract_id VARCHAR, seq INTEGER, type VARCHAR, ts VARCHAR, payload VARCHAR)")
    rows = [json.loads(x) for x in (d / "position_history.jsonl").read_text().splitlines()]
    con.executemany("INSERT INTO position_history VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    [[r[k] for k in ("contract_id", "token_id", "holder", "currency", "status", "outstanding_principal",
                                     "arrears_amount", "periods_paid", "periods_missed", "term_months", "version",
                                     "last_event_id", "updated_at")] for r in rows])
    ev = [json.loads(x) for x in (d / "applied_events.jsonl").read_text().splitlines()]
    con.executemany("INSERT INTO applied_events VALUES (?,?,?,?,?,?)",
                    [[e[k] for k in ("event_id", "contract_id", "seq", "type", "ts", "payload")] for e in ev])

    sql = _sql_for_duckdb((ROOT / "sql" / "views.sql").read_text())
    for stmt in (s for s in sql.split(";") if "CREATE" in s):
        con.execute(stmt)

    latest = con.execute("SELECT COUNT(*) FROM positions_latest").fetchone()[0]
    assert latest == len({r["contract_id"] for r in rows})
    assert con.execute("SELECT COUNT(*) FROM control_version_vs_events").fetchone()[0] == 0
    live = con.execute("SELECT SUM(live_contracts) FROM portfolio_by_holder").fetchone()[0]
    expected = con.execute("SELECT COUNT(*) FROM positions_latest WHERE status IN ('ACTIVE','ARREARS')").fetchone()[0]
    assert live == expected
