#!/usr/bin/env python3
"""
setup_gcs_data.py
=================
Downloads NYC Yellow Taxi Parquet files from TLC public registry
and uploads them to your GCS bucket under gs://<BUCKET>/raw/

Usage:
    python scripts/setup_gcs_data.py --bucket my-taxi-bucket --year 2023 --months 01 02 03

Prerequisites:
    pip install google-cloud-storage requests tqdm
    gcloud auth application-default login
"""

import argparse
import logging
import os
import tempfile

import requests
from google.cloud import storage
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def download_and_upload(bucket_name: str, year: str, month: str) -> None:
    filename = f"yellow_tripdata_{year}-{month}.parquet"
    url = f"{TLC_BASE}/{filename}"
    gcs_path = f"raw/{filename}"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    if blob.exists():
        logger.info(f"Already exists in GCS: gs://{bucket_name}/{gcs_path} — skipping")
        return

    logger.info(f"Downloading {url} ...")
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))

        with open(tmp_path, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=filename
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))

        file_size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
        logger.info(f"Downloaded {file_size_mb:.1f} MB — uploading to GCS...")

        blob.upload_from_filename(tmp_path)
        logger.info(f"Uploaded to gs://{bucket_name}/{gcs_path}")

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--year", default="2023")
    parser.add_argument("--months", nargs="+", default=["01", "02", "03"],
                        help="Months to download (space-separated, zero-padded)")
    args = parser.parse_args()

    logger.info(f"Uploading {len(args.months)} month(s) to gs://{args.bucket}/raw/")
    for month in args.months:
        try:
            download_and_upload(args.bucket, args.year, month)
        except Exception as e:
            logger.error(f"Failed for {args.year}-{month}: {e}")

    logger.info("Done.")


if __name__ == "__main__":
    main()
