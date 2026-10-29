#!/usr/bin/env bash
# Creates the GCP resources for the demo. NOT yet run against a live project by the author:
# review it, run it in a sandbox project, and expect to fix small things.
#
# Usage:  PROJECT=my-project ./infra/setup.sh
# Optional: KMS_KEY=projects/P/locations/europe-west6/keyRings/R/cryptoKeys/K  (CMEK for BigQuery)
set -euo pipefail

: "${PROJECT:?set PROJECT}"
REGION="${REGION:-europe-west6}"          # Zurich, for Swiss data residency
LOCATION="${BQ_LOCATION:-europe-west6}"
DATASET="${DATASET:-lease_demo}"
TOPIC="${TOPIC:-lease-events}"
SUB="${SUB:-lease-events-dataflow}"
DLQ_TOPIC="${DLQ_TOPIC:-lease-events-dlq}"
SA="lease-dataflow@${PROJECT}.iam.gserviceaccount.com"
BUCKET="gs://${PROJECT}-lease-demo-dataflow"

gcloud config set project "$PROJECT"
gcloud services enable pubsub.googleapis.com dataflow.googleapis.com bigquery.googleapis.com \
  compute.googleapis.com storage.googleapis.com

# Messaging: main topic, dead letter topic, and a pull subscription with dead lettering.
gcloud pubsub topics create "$TOPIC" --message-storage-policy-allowed-regions="$REGION" || true
gcloud pubsub topics create "$DLQ_TOPIC" --message-storage-policy-allowed-regions="$REGION" || true
gcloud pubsub subscriptions create "$SUB" --topic "$TOPIC" --ack-deadline=60 \
  --dead-letter-topic="$DLQ_TOPIC" --max-delivery-attempts=5 || true

# Warehouse, optionally encrypted with a customer-managed key.
KMS_ARGS=()
if [[ -n "${KMS_KEY:-}" ]]; then KMS_ARGS=(--default_kms_key "$KMS_KEY"); fi
bq --location="$LOCATION" mk --dataset "${KMS_ARGS[@]}" "${PROJECT}:${DATASET}" || true

bq mk --table --time_partitioning_field updated_at --clustering_fields contract_id \
  "${PROJECT}:${DATASET}.position_history" \
  contract_id:STRING,token_id:STRING,holder:STRING,currency:STRING,status:STRING,outstanding_principal:NUMERIC,arrears_amount:NUMERIC,periods_paid:INTEGER,periods_missed:INTEGER,term_months:INTEGER,version:INTEGER,last_event_id:STRING,updated_at:TIMESTAMP || true
bq mk --table --time_partitioning_field ts --clustering_fields contract_id \
  "${PROJECT}:${DATASET}.applied_events" \
  event_id:STRING,contract_id:STRING,seq:INTEGER,type:STRING,ts:TIMESTAMP,payload:STRING || true
bq mk --table "${PROJECT}:${DATASET}.dead_letter" event_id:STRING,contract_id:STRING,reason:STRING,raw:STRING || true

# Views (placeholder substitution keeps the SQL file portable).
sed "s/\${DATASET}/${PROJECT}.${DATASET}/g" sql/views.sql | bq query --use_legacy_sql=false --location="$LOCATION"

# Least-privilege service account for the Dataflow workers.
gcloud iam service-accounts create lease-dataflow --display-name "Lease demo Dataflow worker" || true
for role in roles/dataflow.worker roles/pubsub.subscriber roles/bigquery.dataEditor roles/bigquery.jobUser roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:${SA}" --role "$role" --condition=None >/dev/null
done
gcloud storage buckets create "$BUCKET" --location="$REGION" --uniform-bucket-level-access || true

echo "Done. Next: ./infra/run_dataflow.sh then python infra/publish.py"
