"""
tests/test_etl_pipeline.py
===========================
Unit tests for NYC Taxi ETL transformations.
Run with: pytest tests/ -v

Requires:
    pip install pytest pyspark
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from pyspark_jobs.etl_pipeline import clean_data, transform_data, build_daily_aggregates


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests — local mode, no cluster needed.
    Windows: uses local[1] + loopback bind + extended timeouts to avoid Py4J socket issues.
    """
    # Tell PySpark to use the same Python executable running pytest
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    session = (
        SparkSession.builder
        .appName("test_nyc_taxi_etl")
        .master("local[1]")                          # 1 core avoids Windows worker spawn issues
        .config("spark.sql.shuffle.partitions", "1") # minimum shuffles for tiny test data
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.python.worker.reuse", "false") # fresh worker per task
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")    # force loopback — key fix on Windows
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_trip_row(**overrides):
    """Helper: returns a valid base trip row with optional overrides."""
    base = {
        "tpep_pickup_datetime": datetime(2023, 6, 15, 9, 0, 0),
        "tpep_dropoff_datetime": datetime(2023, 6, 15, 9, 22, 0),
        "PULocationID": 161,
        "DOLocationID": 237,
        "passenger_count": 1,
        "trip_distance": 3.5,
        "fare_amount": 15.00,
        "tip_amount": 3.00,
        "total_amount": 19.50,
        "payment_type": 1,
        "RatecodeID": 1,
        "VendorID": 1,
        "store_and_fwd_flag": "N",
        "extra": 0.5,
        "mta_tax": 0.5,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "congestion_surcharge": 2.5,
    }
    base.update(overrides)
    return Row(**base)


class TestCleanData:
    def test_valid_row_passes(self, spark):
        df = spark.createDataFrame([make_trip_row()])
        clean_df, metrics = clean_data(df)
        assert clean_df.count() == 1
        assert metrics["dropped_rows"] == 0

    def test_negative_fare_dropped(self, spark):
        df = spark.createDataFrame([make_trip_row(fare_amount=-5.0)])
        clean_df, metrics = clean_data(df)
        assert clean_df.count() == 0
        assert metrics["dropped_rows"] == 1

    def test_zero_distance_dropped(self, spark):
        df = spark.createDataFrame([make_trip_row(trip_distance=0.0)])
        clean_df, metrics = clean_data(df)
        assert clean_df.count() == 0

    def test_dropoff_before_pickup_dropped(self, spark):
        df = spark.createDataFrame([make_trip_row(
            tpep_pickup_datetime=datetime(2023, 6, 15, 10, 0),
            tpep_dropoff_datetime=datetime(2023, 6, 15, 9, 0),
        )])
        clean_df, _ = clean_data(df)
        assert clean_df.count() == 0

    def test_invalid_passenger_count_dropped(self, spark):
        df = spark.createDataFrame([make_trip_row(passenger_count=0)])
        clean_df, _ = clean_data(df)
        assert clean_df.count() == 0

    def test_drop_rate_calculated(self, spark):
        rows = [make_trip_row(), make_trip_row(fare_amount=-1.0)]
        df = spark.createDataFrame(rows)
        _, metrics = clean_data(df)
        assert metrics["drop_rate_pct"] == 50.0

    def test_excessive_fare_dropped(self, spark):
        df = spark.createDataFrame([make_trip_row(fare_amount=1500.0)])
        clean_df, _ = clean_data(df)
        assert clean_df.count() == 0


# ── KEY FIX: run transform_data() ONCE, share result across all 6 tests ───────
# Each test previously called transform_data() independently, triggering the
# 7-day rolling window function 6 times — that's what was hanging on Windows.
@pytest.fixture(scope="module")
def transformed_row(spark):
    """
    Runs clean_data + transform_data once, collects to a plain Python dict.
    All TestTransformData tests read from this dict — zero extra Spark actions.
    """
    df = spark.createDataFrame([make_trip_row()])
    clean_df, _ = clean_data(df)
    result = transform_data(clean_df)
    row = result.collect()[0]  # one Spark action total
    return row.asDict()


class TestTransformData:
    def test_trip_duration_computed(self, transformed_row):
        assert transformed_row["trip_duration_minutes"] == pytest.approx(22.0, abs=0.1)

    def test_time_of_day_morning(self, transformed_row):
        # pickup_hour=9 → morning
        assert transformed_row["time_of_day"] == "morning"

    def test_trip_category_medium(self, transformed_row):
        # 3.5 miles → medium
        assert transformed_row["trip_category"] == "medium"

    def test_tip_pct_calculated(self, transformed_row):
        # tip=3.00, fare=15.00 → 20%
        assert transformed_row["tip_pct"] == pytest.approx(20.0, abs=0.1)

    def test_payment_label_credit_card(self, transformed_row):
        assert transformed_row["payment_label"] == "credit_card"

    def test_is_weekend_false_for_weekday(self, transformed_row):
        # 2023-06-15 is a Thursday
        assert transformed_row["is_weekend"] is False


class TestAggregates:
    def test_aggregate_schema(self, spark):
        df = spark.createDataFrame([make_trip_row()])
        clean_df, _ = clean_data(df)
        transformed = transform_data(clean_df)
        agg = build_daily_aggregates(transformed)
        expected_cols = {
            "pickup_date", "pickup_hour", "time_of_day", "trip_category",
            "payment_label", "trip_count", "total_revenue", "avg_fare",
            "avg_distance_miles", "avg_duration_minutes", "avg_tip_pct",
        }
        assert expected_cols.issubset(set(agg.columns))

    def test_trip_count_correct(self, spark):
        rows = [make_trip_row() for _ in range(3)]
        df = spark.createDataFrame(rows)
        clean_df, _ = clean_data(df)
        transformed = transform_data(clean_df)
        agg = build_daily_aggregates(transformed)
        total_trips = agg.agg({"trip_count": "sum"}).collect()[0][0]
        assert total_trips == 3
