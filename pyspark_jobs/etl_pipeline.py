"""
NYC Taxi Trip Data - PySpark ETL Pipeline
==========================================
Stage 1: Ingest raw Parquet from GCS → Clean → Transform → Load to BigQuery

Run on GCP Dataproc:
    gcloud dataproc jobs submit pyspark gs://<BUCKET>/pyspark_jobs/etl_pipeline.py \
        --cluster=nyc-taxi-cluster \
        --region=us-central1 \
        --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
        -- --project=<PROJECT_ID> --bucket=<BUCKET> --year=2023 --month=01
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
NYC_TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BQ_DATASET = "nyc_taxi"
VALID_PAYMENT_TYPES = [1, 2, 3, 4, 5, 6]
MAX_TRIP_HOURS = 24
MAX_FARE_USD = 1000
MAX_DISTANCE_MILES = 200
BOROUGH_LOOKUP = {
    # Simplified zone→borough mapping (full lookup in zone_lookup table)
    "Manhattan": [4, 12, 13, 24, 41, 42, 43, 45, 48, 50, 68, 74, 75, 79, 87, 88,
                  90, 100, 103, 104, 105, 107, 113, 114, 116, 120, 125, 127, 128,
                  137, 140, 141, 142, 143, 144, 148, 151, 152, 153, 158, 161, 162,
                  163, 164, 166, 170, 186, 194, 202, 209, 211, 224, 229, 230, 231,
                  232, 233, 234, 236, 237, 238, 239, 243, 244, 246, 249, 261, 262],
}


def create_spark_session(project_id: str, bucket: str) -> SparkSession:
    """Create SparkSession with BigQuery connector config."""
    spark = (
        SparkSession.builder
        .appName("NYC_Taxi_ETL_Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("temporaryGcsBucket", bucket)
        .config("parentProject", project_id)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created — Spark version: {spark.version}")
    return spark


# ── STAGE 1: INGEST ────────────────────────────────────────────────────────────

def ingest_raw_data(spark: SparkSession, bucket: str, year: str, month: str):
    """
    Read Yellow Taxi Parquet from GCS.
    Source: NYC TLC — https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    Files are ~500MB–1GB each, containing 3–8M rows per month.
    """
    gcs_path = f"gs://{bucket}/raw/yellow_tripdata_{year}-{month}.parquet"
    logger.info(f"Ingesting raw data from: {gcs_path}")

    df = spark.read.parquet(gcs_path)
    row_count = df.count()
    logger.info(f"Ingested {row_count:,} raw rows for {year}-{month}")

    # Persist schema to logs for audit trail
    logger.info(f"Schema:\n{df.schema.simpleString()}")
    return df


# ── STAGE 2: CLEAN ─────────────────────────────────────────────────────────────

def clean_data(df):
    """
    Apply data quality rules:
    - Drop nulls on key columns
    - Filter out impossible/corrupt values
    - Standardise column names to snake_case
    - Cast columns to correct types
    Returns cleaned df + quality metrics dict
    """
    raw_count = df.count()
    logger.info(f"Starting data cleaning on {raw_count:,} rows")

    # Rename to snake_case (TLC schema uses mixed case)
    df = (df
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("VendorID", "vendor_id")
    )

    # Cast types
    df = (df
        .withColumn("pickup_datetime", F.col("pickup_datetime").cast(TimestampType()))
        .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
        .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    )

    # Drop rows with null in critical columns
    critical_cols = [
        "pickup_datetime", "dropoff_datetime", "pickup_location_id",
        "dropoff_location_id", "trip_distance", "fare_amount", "total_amount"
    ]
    df = df.dropna(subset=critical_cols)

    # Business rule filters
    df = (df
        # Drop zero/negative fares (except valid $0 trips like free rides)
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("total_amount") >= 0)
        # Cap unrealistically large fares
        .filter(F.col("fare_amount") <= MAX_FARE_USD)
        # Valid passenger count (1–9)
        .filter((F.col("passenger_count") >= 1) & (F.col("passenger_count") <= 9))
        # Trip distance must be positive and realistic
        .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") <= MAX_DISTANCE_MILES))
        # Dropoff must be after pickup
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        # Valid payment types
        .filter(F.col("payment_type").isin(VALID_PAYMENT_TYPES))
        # Drop future-dated or very old records (data error)
        .filter(F.col("pickup_datetime") >= F.lit("2009-01-01"))
        .filter(F.col("pickup_datetime") < F.lit("2026-01-01"))
    )

    # Trip duration filter (must be < MAX_TRIP_HOURS hours)
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60
    )
    df = df.filter(
        (F.col("trip_duration_minutes") > 0) &
        (F.col("trip_duration_minutes") <= MAX_TRIP_HOURS * 60)
    )

    clean_count = df.count()
    dropped = raw_count - clean_count
    quality_metrics = {
        "raw_rows": raw_count,
        "clean_rows": clean_count,
        "dropped_rows": dropped,
        "drop_rate_pct": round((dropped / raw_count) * 100, 2)
    }
    logger.info(f"Cleaning complete: {clean_count:,} rows kept, {dropped:,} dropped ({quality_metrics['drop_rate_pct']}%)")
    return df, quality_metrics


# ── STAGE 3: TRANSFORM ─────────────────────────────────────────────────────────

def transform_data(df):
    """
    Engineer features used in downstream analytics:
    - Time dimensions (hour, day_of_week, is_weekend, time_of_day_bucket)
    - Speed, cost-per-mile, tip percentage
    - Rolling 7-day average fare per pickup zone (window function)
    - Trip category labels
    """
    logger.info("Running transformations...")

    df = (df
        # ── Time features ──
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))   # 1=Sun
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("is_weekend", F.col("pickup_day_of_week").isin([1, 7]).cast("boolean"))
        .withColumn("time_of_day",
            F.when(F.col("pickup_hour").between(6, 11), "morning")
             .when(F.col("pickup_hour").between(12, 16), "afternoon")
             .when(F.col("pickup_hour").between(17, 20), "evening_rush")
             .when(F.col("pickup_hour").between(21, 23), "night")
             .otherwise("late_night")
        )

        # ── Derived metrics ──
        .withColumn("speed_mph",
            F.round(
                F.col("trip_distance") / (F.col("trip_duration_minutes") / 60), 2
            )
        )
        .withColumn("cost_per_mile",
            F.round(
                F.when(F.col("trip_distance") > 0,
                       F.col("total_amount") / F.col("trip_distance"))
                 .otherwise(F.lit(None)), 2
            )
        )
        .withColumn("tip_pct",
            F.round(
                F.when(F.col("fare_amount") > 0,
                       (F.col("tip_amount") / F.col("fare_amount")) * 100)
                 .otherwise(F.lit(0.0)), 2
            )
        )

        # ── Trip category ──
        .withColumn("trip_category",
            F.when(F.col("trip_distance") < 1.0, "short")
             .when(F.col("trip_distance").between(1.0, 5.0), "medium")
             .when(F.col("trip_distance").between(5.0, 15.0), "long")
             .otherwise("very_long")
        )

        # ── Payment label ──
        .withColumn("payment_label",
            F.when(F.col("payment_type") == 1, "credit_card")
             .when(F.col("payment_type") == 2, "cash")
             .when(F.col("payment_type") == 3, "no_charge")
             .when(F.col("payment_type") == 4, "dispute")
             .otherwise("unknown")
        )
    )

    # ── Window: 7-day rolling avg fare per pickup zone ──
    # Useful for anomaly detection and zone-level pricing trends
    # FIX: PySpark 3.4+ does not allow DATE.cast("long").
    # Use unix_date() (days since epoch) and rangeBetween in days instead.
    window_7d = (
        Window
        .partitionBy("pickup_location_id")
        .orderBy(F.unix_date(F.col("pickup_date")))   # days since epoch — compatible with PySpark 3.4+
        .rangeBetween(-7, 0)                           # 7 days (in day units, matching unix_date)
    )
    df = df.withColumn(
        "rolling_7d_avg_fare_by_zone",
        F.round(F.avg("fare_amount").over(window_7d), 2)
    )

    logger.info("Transformations complete")
    return df


# ── STAGE 4: AGGREGATE → FACT TABLE ───────────────────────────────────────────

def build_daily_aggregates(df):
    """
    Build a daily KPI summary table:
    - Total trips, revenue, avg fare, avg distance, avg tip %
    - Grouped by pickup_date + time_of_day + trip_category
    This is what goes into dashboards / BI tools.
    """
    logger.info("Building daily aggregate table...")

    agg_df = (df
        .groupBy("pickup_date", "pickup_hour", "time_of_day", "trip_category", "payment_label")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
            F.round(F.avg("speed_mph"), 2).alias("avg_speed_mph"),
            F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"),
        )
        .orderBy("pickup_date", "pickup_hour")
    )

    logger.info(f"Aggregate table: {agg_df.count():,} rows")
    return agg_df


# ── STAGE 5: LOAD → BIGQUERY ──────────────────────────────────────────────────

def load_to_bigquery(df, project_id: str, table_name: str, mode: str = "append"):
    """
    Write DataFrame to BigQuery using the Spark-BigQuery connector.
    mode: 'overwrite' for full reload, 'append' for incremental.
    """
    bq_table = f"{project_id}.{BQ_DATASET}.{table_name}"
    logger.info(f"Writing {df.count():,} rows to BigQuery: {bq_table} (mode={mode})")

    (df.write
        .format("bigquery")
        .option("table", bq_table)
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeDisposition", "WRITE_APPEND" if mode == "append" else "WRITE_TRUNCATE")
        .save()
    )
    logger.info(f"Successfully loaded to {bq_table}")


# ── STAGE 6: DATA QUALITY REPORT ──────────────────────────────────────────────

def write_quality_metrics(spark, project_id: str, metrics: dict, year: str, month: str):
    """Write quality metrics to BigQuery for pipeline monitoring."""
    metrics_row = [{
        "run_timestamp": datetime.utcnow().isoformat(),
        "partition": f"{year}-{month}",
        "raw_rows": metrics["raw_rows"],
        "clean_rows": metrics["clean_rows"],
        "dropped_rows": metrics["dropped_rows"],
        "drop_rate_pct": metrics["drop_rate_pct"],
    }]
    metrics_df = spark.createDataFrame(metrics_row)
    load_to_bigquery(metrics_df, project_id, "pipeline_quality_log", mode="append")
    logger.info("Quality metrics written to BigQuery")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="NYC Taxi PySpark ETL")
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument("--bucket", required=True, help="GCS Bucket name")
    parser.add_argument("--year", default="2023", help="Trip data year")
    parser.add_argument("--month", default="01", help="Trip data month (zero-padded)")
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"])
    args = parser.parse_args()

    logger.info(f"=== NYC Taxi ETL Pipeline START | {args.year}-{args.month} ===")
    start_time = datetime.utcnow()

    spark = create_spark_session(args.project, args.bucket)

    # Pipeline stages
    raw_df = ingest_raw_data(spark, args.bucket, args.year, args.month)
    clean_df, quality_metrics = clean_data(raw_df)
    transformed_df = transform_data(clean_df)
    agg_df = build_daily_aggregates(transformed_df)

    # Load both granular + aggregate tables
    load_to_bigquery(transformed_df, args.project, f"trips_clean_{args.year}_{args.month}", mode=args.mode)
    load_to_bigquery(agg_df, args.project, "trips_daily_kpis", mode=args.mode)
    write_quality_metrics(spark, args.project, quality_metrics, args.year, args.month)

    elapsed = (datetime.utcnow() - start_time).total_seconds()
    logger.info(f"=== Pipeline COMPLETE in {elapsed:.1f}s ===")
    logger.info(f"Quality summary: {quality_metrics}")

    spark.stop()


if __name__ == "__main__":
    main()
