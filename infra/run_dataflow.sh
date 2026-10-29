#!/usr/bin/env bash
# Launches the streaming pipeline on Dataflow. Not yet run against a live project by the author.
# Usage: PROJECT=my-project ./infra/run_dataflow.sh   (optional KMS_KEY for CMEK on Dataflow resources)
set -euo pipefail
: "${PROJECT:?set PROJECT}"
REGION="${REGION:-europe-west6}"
DATASET="${DATASET:-lease_demo}"
SUB="${SUB:-lease-events-dataflow}"
SA="lease-dataflow@${PROJECT}.iam.gserviceaccount.com"
BUCKET="gs://${PROJECT}-lease-demo-dataflow"

EXTRA=()
if [[ -n "${KMS_KEY:-}" ]]; then EXTRA=(--dataflow_kms_key "$KMS_KEY"); fi

PYTHONPATH=src python -m lease_demo.pipeline \
  --subscription "projects/${PROJECT}/subscriptions/${SUB}" \
  --bq_dataset "${PROJECT}:${DATASET}" \
  --runner DataflowRunner --streaming \
  --project "$PROJECT" --region "$REGION" \
  --temp_location "${BUCKET}/tmp" --staging_location "${BUCKET}/staging" \
  --service_account_email "$SA" \
  --no_use_public_ips --max_num_workers 3 \
  --setup_file ./setup.py \
  "${EXTRA[@]}"
