"""Apache Beam pipeline: lease token events -> position keeping -> BigQuery.

    Pub/Sub (or JSONL file) -> parse/validate -> key by contract_id
        -> stateful position keeper (one state cell per contract)
        -> BigQuery: position_history, applied_events, dead_letter

Runs unchanged on the DirectRunner (local) and the DataflowRunner (GCP).
"""
from __future__ import annotations

import argparse
import json
import logging

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.pipeline import PipelineOptions
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec

from . import ledger

APPLIED, REJECTED = "applied", "rejected"


class ParseEvent(beam.DoFn):
    """Bytes/str -> (contract_id, event). Malformed input goes to the dead letter."""

    def process(self, raw):
        text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        try:
            ev = json.loads(text)
            ledger.validate_envelope(ev)
            yield ev["contract_id"], ev
        except (ValueError, ledger.Rejected, TypeError) as e:
            yield beam.pvalue.TaggedOutput(
                REJECTED,
                {"event_id": None, "contract_id": None, "reason": f"parse: {e}", "raw": text[:2000]},
            )


class PositionKeeper(beam.DoFn):
    """One state cell per contract. All business rules live in ledger.step."""

    STATE = ReadModifyWriteStateSpec("contract", StrUtf8Coder())

    def process(self, element, state=beam.DoFn.StateParam(STATE)):
        _, ev = element
        raw = state.read()
        current = ledger.ContractState.from_json(raw) if raw else None
        new_state, applied, rejected = ledger.step(current, ev)
        if new_state is not None:
            state.write(new_state.to_json())
        for applied_ev, snapshot in applied:
            yield beam.pvalue.TaggedOutput(APPLIED, {
                "event": {
                    "event_id": applied_ev["event_id"], "contract_id": applied_ev["contract_id"],
                    "seq": applied_ev["seq"], "type": applied_ev["type"], "ts": applied_ev["ts"],
                    "payload": json.dumps(applied_ev.get("payload", {}), sort_keys=True),
                },
                "position": ledger.position_row(snapshot),
            })
        for rej_ev, reason in rejected:
            yield beam.pvalue.TaggedOutput(REJECTED, {
                "event_id": rej_ev.get("event_id"), "contract_id": rej_ev.get("contract_id"),
                "reason": reason, "raw": json.dumps(rej_ev)[:2000],
            })


def build(p, source, sinks):
    """Wire the graph. `source` yields raw lines, `sinks` maps name -> PTransform."""
    parsed = (
        source
        | "Parse" >> beam.ParDo(ParseEvent()).with_outputs(REJECTED, main="ok")
    )
    kept = (
        parsed.ok  # already keyed by contract_id
        | "PositionKeeper" >> beam.ParDo(PositionKeeper()).with_outputs(APPLIED, REJECTED, main="none")
    )
    applied = kept[APPLIED]
    _ = applied | "Positions" >> beam.Map(lambda x: x["position"]) | "WritePositions" >> sinks["position_history"]
    _ = applied | "Events" >> beam.Map(lambda x: x["event"]) | "WriteEvents" >> sinks["applied_events"]
    _ = (
        (parsed[REJECTED], kept[REJECTED])
        | "MergeRejects" >> beam.Flatten()
        | "WriteDeadLetter" >> sinks["dead_letter"]
    )


BQ_SCHEMAS = {
    "position_history": (
        "contract_id:STRING,token_id:STRING,holder:STRING,currency:STRING,status:STRING,"
        "outstanding_principal:NUMERIC,arrears_amount:NUMERIC,periods_paid:INTEGER,"
        "periods_missed:INTEGER,term_months:INTEGER,version:INTEGER,last_event_id:STRING,"
        "updated_at:TIMESTAMP"
    ),
    "applied_events": "event_id:STRING,contract_id:STRING,seq:INTEGER,type:STRING,ts:TIMESTAMP,payload:STRING",
    "dead_letter": "event_id:STRING,contract_id:STRING,reason:STRING,raw:STRING",
}


def run(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="JSONL file (batch/local mode)")
    ap.add_argument("--subscription", help="projects/<p>/subscriptions/<s> (streaming mode)")
    ap.add_argument("--output_dir", help="local mode: write JSONL files here")
    ap.add_argument("--bq_dataset", help="project:dataset for BigQuery output")
    known, beam_args = ap.parse_known_args(argv)
    opts = PipelineOptions(beam_args, save_main_session=True)
    if known.subscription:
        opts.view_as(beam.options.pipeline_options.StandardOptions).streaming = True

    with beam.Pipeline(options=opts) as p:
        if known.subscription:
            source = p | "ReadPubSub" >> ReadFromPubSub(subscription=known.subscription)
        else:
            source = p | "ReadFile" >> beam.io.ReadFromText(known.input)

        sinks = {}
        for name, schema in BQ_SCHEMAS.items():
            if known.bq_dataset:
                sinks[name] = WriteToBigQuery(
                    table=f"{known.bq_dataset}.{name}", schema=schema,
                    create_disposition=BigQueryDisposition.CREATE_NEVER,
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    method=WriteToBigQuery.Method.STREAMING_INSERTS,
                )
            else:
                sinks[name] = beam.Map(json.dumps) | beam.io.WriteToText(
                    f"{known.output_dir}/{name}", file_name_suffix=".jsonl", shard_name_template="")
        build(p, source, sinks)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
