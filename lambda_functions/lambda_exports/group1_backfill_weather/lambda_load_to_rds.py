# lambda_load_to_rds.py
import os
import boto3
import psycopg2
import pandas as pd
from io import StringIO
from s3_to_rds import load_all_cities, load_aggregated_weather

# Config from environment
bucket    = os.environ["S3_BUCKET"]
PG_CONFIG = {
    "host":     os.environ["PG_HOST"],
    "port":     int(os.environ.get("PG_PORT", 5432)),
    "dbname":   os.environ["PG_DB"],
    "user":     os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}

CITIES = ["Zurich","Geneva","Basel","Lausanne","Bern"]

def lambda_handler(event, context):
    # 1) Load per-city tables
    load_all_cities(CITIES, bucket, PG_CONFIG)

    # 2) Load aggregated file into RDS
    load_aggregated_weather(
        bucket=bucket,
        key="weather_backfill/aggregated_weather.csv",
        pg_config=PG_CONFIG
    )

    return {
        "statusCode": 200,
        "body": "Per-city and aggregated data loaded into RDS"
    }
