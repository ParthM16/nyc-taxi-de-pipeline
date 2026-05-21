#!/bin/bash
# =============================================================================
# run_pipeline.sh
# Submits the PySpark ETL job to Dataproc for one or multiple months.
#
# Usage:
#   Single month:   bash scripts/run_pipeline.sh 2023 01
#   Multiple months: for m in 01 02 03; do bash scripts/run_pipeline.sh 2023 $m; done
# =============================================================================

set -euo pipefail

PROJECT_ID="your-gcp-project-id"          # <-- replace
BUCKET_NAME="nyc-taxi-de-${PROJECT_ID}"
REGION="us-central1"
CLUSTER_NAME="nyc-taxi-cluster"

YEAR="${1:-2023}"
MONTH="${2:-01}"

echo "=== Submitting PySpark job: ${YEAR}-${MONTH} ==="

gcloud dataproc jobs submit pyspark \
  "gs://${BUCKET_NAME}/pyspark_jobs/etl_pipeline.py" \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --jars="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar" \
  --properties="spark.executor.memory=4g,spark.driver.memory=4g" \
  -- \
  --project="${PROJECT_ID}" \
  --bucket="${BUCKET_NAME}" \
  --year="${YEAR}" \
  --month="${MONTH}" \
  --mode="append"

echo "=== Job submitted for ${YEAR}-${MONTH} ==="
echo "Check logs: https://console.cloud.google.com/dataproc/jobs?project=${PROJECT_ID}"
