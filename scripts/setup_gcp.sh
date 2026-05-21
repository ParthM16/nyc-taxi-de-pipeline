#!/bin/bash
# =============================================================================
# setup_gcp.sh
# Creates all GCP resources needed for the NYC Taxi DE pipeline.
# Run ONCE before your first pipeline execution.
#
# Usage: bash scripts/setup_gcp.sh
# Prerequisites: gcloud CLI installed + authenticated
# =============================================================================

set -euo pipefail

# ── CONFIG — edit these ───────────────────────────────────────────────────────
PROJECT_ID="your-gcp-project-id"          # <-- replace
BUCKET_NAME="nyc-taxi-de-${PROJECT_ID}"   # globally unique
REGION="us-central1"
ZONE="us-central1-a"
CLUSTER_NAME="nyc-taxi-cluster"
BQ_DATASET="nyc_taxi"
# ─────────────────────────────────────────────────────────────────────────────

echo "=== Setting project ==="
gcloud config set project "${PROJECT_ID}"

echo "=== Enabling APIs ==="
gcloud services enable \
  dataproc.googleapis.com \
  bigquery.googleapis.com \
  storage-component.googleapis.com \
  bigquerystorage.googleapis.com \
  compute.googleapis.com

echo "=== Creating GCS bucket: gs://${BUCKET_NAME} ==="
gcloud storage buckets create "gs://${BUCKET_NAME}" \
  --location="${REGION}" \
  --uniform-bucket-level-access \
  || echo "Bucket already exists — skipping"

# Create folders
gcloud storage cp /dev/null "gs://${BUCKET_NAME}/raw/.keep"
gcloud storage cp /dev/null "gs://${BUCKET_NAME}/pyspark_jobs/.keep"
gcloud storage cp /dev/null "gs://${BUCKET_NAME}/logs/.keep"

echo "=== Uploading PySpark job to GCS ==="
gcloud storage cp pyspark_jobs/etl_pipeline.py \
  "gs://${BUCKET_NAME}/pyspark_jobs/etl_pipeline.py"

echo "=== Creating BigQuery dataset: ${BQ_DATASET} ==="
bq --location="${REGION}" mk \
  --dataset \
  --description "NYC Taxi DE Pipeline" \
  "${PROJECT_ID}:${BQ_DATASET}" \
  || echo "Dataset already exists — skipping"

echo "=== Creating Dataproc cluster: ${CLUSTER_NAME} ==="
# Single-region ephemeral cluster — STOP it after jobs to save credits
gcloud dataproc clusters create "${CLUSTER_NAME}" \
  --region="${REGION}" \
  --zone="${ZONE}" \
  --master-machine-type=n2-standard-4 \
  --master-boot-disk-size=50GB \
  --num-workers=2 \
  --worker-machine-type=n2-standard-4 \
  --worker-boot-disk-size=50GB \
  --image-version=2.1-debian11 \
  --optional-components=JUPYTER \
  --enable-component-gateway \
  --project="${PROJECT_ID}"

echo ""
echo "=== SETUP COMPLETE ==="
echo "Bucket:   gs://${BUCKET_NAME}"
echo "BQ:       ${PROJECT_ID}.${BQ_DATASET}"
echo "Cluster:  ${CLUSTER_NAME} (${REGION})"
echo ""
echo "Next: run scripts/setup_gcs_data.py to download NYC TLC data"
echo "Then: bash scripts/run_pipeline.sh to execute the ETL"
echo ""
echo "IMPORTANT: Delete cluster when done to save GCP credits:"
echo "  gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGION}"
