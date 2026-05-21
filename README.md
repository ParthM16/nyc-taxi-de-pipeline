# NYC Taxi Data Engineering Pipeline

End-to-end data engineering project processing **30M+ NYC Yellow Taxi trip records** using PySpark on GCP Dataproc, with results loaded to BigQuery for BI analysis.

> **Live project** — built incrementally. Each stage adds a new tool from the modern DE stack.  
> Current: Stage 1 (PySpark + GCP). Upcoming: Snowflake, dbt, Kafka streaming.

---

## Architecture

```
NYC TLC Public Data (Parquet)
         │
         ▼
   GCS Raw Storage              ← Cloud Storage (always-free tier)
         │
         ▼
  Dataproc (PySpark)            ← Managed Spark cluster
   ┌─────────────────────┐
   │  1. Ingest           │     read_parquet from GCS
   │  2. Clean            │     null drops, business rule filters
   │  3. Transform        │     feature engineering + window functions
   │  4. Aggregate        │     daily KPI rollups
   └─────────────────────┘
         │
         ▼
     BigQuery                   ← Analytics warehouse (always-free 1TB/mo)
   ┌──────────────────────────────────┐
   │  nyc_taxi.trips_clean_YYYY_MM    │  granular cleaned trips
   │  nyc_taxi.trips_daily_kpis       │  daily aggregates for dashboards
   │  nyc_taxi.pipeline_quality_log   │  data quality audit trail
   └──────────────────────────────────┘
         │
         ▼
  Apache Airflow DAG             ← Monthly schedule (2nd of each month)
```

---

## Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Processing | **PySpark 3.5** on GCP Dataproc | Distributed compute for 3–8M rows/month |
| Storage | **GCS** (raw) + **BigQuery** (warehouse) | Both covered by GCP free tier |
| Orchestration | **Apache Airflow** | Industry-standard DAG scheduling |
| Language | **Python 3.11** | ETL logic, data download scripts |
| Testing | **pytest** + **pyspark local mode** | Unit tests run without a cluster |
| CI | GitHub Actions | Auto-run tests on every push |

---

## Data

**Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — public dataset, updated monthly.

**Scale:** Each monthly Parquet file is 500MB–1GB containing 3–8M rows.  
**Processing scope:** Jan–Dec 2023 = ~60M rows total across 12 months.

**Key columns used:**
- `pickup_datetime`, `dropoff_datetime` — trip time window
- `PULocationID`, `DOLocationID` — 263 NYC taxi zones
- `trip_distance`, `fare_amount`, `tip_amount`, `total_amount`
- `passenger_count`, `payment_type`

---

## Data Quality Rules

The cleaning stage enforces these business rules and logs drop rates per run:

| Rule | Filter |
|---|---|
| Null critical columns | Drop rows with null pickup/dropoff/location/fare |
| Valid fare | `0 ≤ fare_amount ≤ $1,000` |
| Valid distance | `0 < trip_distance ≤ 200 miles` |
| Valid passengers | `1 ≤ passenger_count ≤ 9` |
| Temporal sanity | dropoff > pickup; duration < 24 hours |
| Valid payment types | Types 1–6 only |
| Date range | 2009–2026 (catches corrupt timestamps) |

---

## Engineered Features

| Feature | Description |
|---|---|
| `trip_duration_minutes` | (dropoff - pickup) in minutes |
| `pickup_hour`, `pickup_day_of_week` | Time dimensions |
| `is_weekend` | Boolean flag |
| `time_of_day` | morning / afternoon / evening_rush / night / late_night |
| `speed_mph` | distance / (duration_minutes / 60) |
| `cost_per_mile` | total_amount / trip_distance |
| `tip_pct` | (tip_amount / fare_amount) × 100 |
| `trip_category` | short / medium / long / very_long |
| `payment_label` | credit_card / cash / no_charge / dispute |
| `rolling_7d_avg_fare_by_zone` | 7-day window avg fare per pickup zone |

---

## Setup & Run

### Prerequisites
- GCP account with billing enabled (free $300 trial works)
- `gcloud` CLI installed and authenticated
- Python 3.11+

### 1. Clone and install

```bash
git clone https://github.com/ParthM16/nyc-taxi-de-pipeline.git
cd nyc-taxi-de-pipeline
pip install -r requirements.txt
```

### 2. Configure GCP

Edit `PROJECT_ID` and `BUCKET_NAME` in `scripts/setup_gcp.sh`, then:

```bash
bash scripts/setup_gcp.sh
```

This creates: GCS bucket, BigQuery dataset, Dataproc cluster. Takes ~3 minutes.

### 3. Download data

```bash
python scripts/setup_gcs_data.py \
  --bucket nyc-taxi-de-YOUR-PROJECT \
  --year 2023 \
  --months 01 02 03
```

### 4. Run the ETL

```bash
bash scripts/run_pipeline.sh 2023 01
```

Or run all 12 months:

```bash
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  bash scripts/run_pipeline.sh 2023 $m
done
```

### 5. Run tests locally (no cluster needed)

```bash
pytest tests/ -v
```

### 6. Cleanup (important — stop billing)

```bash
gcloud dataproc clusters delete nyc-taxi-cluster --region=us-central1
```

---

## BigQuery Queries (sample)

```sql
-- Peak hours by total revenue
SELECT
  pickup_hour,
  time_of_day,
  SUM(trip_count) AS total_trips,
  ROUND(SUM(total_revenue), 2) AS revenue
FROM `nyc_taxi.trips_daily_kpis`
GROUP BY 1, 2
ORDER BY revenue DESC;

-- Average tip % by payment type and trip category
SELECT
  payment_label,
  trip_category,
  ROUND(AVG(avg_tip_pct), 2) AS avg_tip_pct
FROM `nyc_taxi.trips_daily_kpis`
GROUP BY 1, 2
ORDER BY avg_tip_pct DESC;

-- Data quality log
SELECT partition, raw_rows, clean_rows, drop_rate_pct, run_timestamp
FROM `nyc_taxi.pipeline_quality_log`
ORDER BY run_timestamp DESC;
```

---

## Project Roadmap

This repo grows as I learn new DE tools. Each stage is a separate commit branch.

- [x] **Stage 1** — PySpark ETL on GCP Dataproc → BigQuery *(current)*
- [ ] **Stage 2** — Add Snowflake as secondary warehouse + dbt transformations + Airflow orchestration
- [ ] **Stage 3** — Real-time streaming layer: Kafka producer → PySpark Structured Streaming → BigQuery

---

## Results

Processing 12 months of 2023 data (Jan–Dec):

| Metric | Value |
|---|---|
| Raw records processed | ~62M rows |
| Records passing quality checks | ~58M (avg ~94%) |
| Processing time per month | ~4–6 min on 2-worker Dataproc cluster |
| GCP cost (from trial credits) | ~$3–5 total |

---

## Author

**Parth Maheshwari** — Data Engineer | [LinkedIn](https://www.linkedin.com/in/maheshwariparth) | [Portfolio](https://parthm16.github.io/) | [GitHub](https://github.com/ParthM16)
