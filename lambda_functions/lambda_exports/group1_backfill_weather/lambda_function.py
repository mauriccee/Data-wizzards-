# ingestion_lambda.py
import os
import boto3
import pandas as pd
from io import StringIO
import openmeteo_requests
import requests_cache
from retry_requests import retry
from transforms import transform_df
from datetime import date, timedelta

# ─── Config ───
CITIES = {
    "Zurich":   (47.3769,  8.5417),
    "Geneva":   (46.2044,  6.1432),
    "Basel":    (47.5596,  7.5886),
    "Lausanne": (46.5197,  6.6323),
    "Bern":     (46.9480,  7.4474),
}
TODAY      = date.today()
START_DATE = TODAY.replace(year=TODAY.year - 3).isoformat()
END_DATE   = (TODAY - timedelta(days=1)).isoformat()

# ─── HTTP client w/ cache & retry ───
cache_sess = requests_cache.CachedSession("/tmp/.cache", expire_after=3600)
retry_sess = retry(cache_sess, retries=5, backoff_factor=0.2)
om_client  = openmeteo_requests.Client(session=retry_sess)

# ─── S3 client ───
s3     = boto3.client("s3")
bucket = os.environ["S3_BUCKET"]

def lambda_handler(event, context):
    city_dfs = []

    # 1) Fetch, transform & upload each city
    for city_name, (lat, lon) in CITIES.items():
        resp = om_client.weather_api(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude":   lat,
                "longitude":  lon,
                "start_date": START_DATE,
                "end_date":   END_DATE,
                "daily": [
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum",
                    "sunshine_duration",
                    "wind_speed_10m_max",
                    "daylight_duration"
                ]
            }
        )[0].Daily()

        dates = pd.date_range(
            start=pd.to_datetime(resp.Time(),    unit="s", utc=True),
            end=  pd.to_datetime(resp.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=resp.Interval()),
            inclusive="left"
        ).date

        df = pd.DataFrame({
            "date":                  dates,
            "temperature_2m_max":    resp.Variables(0).ValuesAsNumpy().tolist(),
            "temperature_2m_min":    resp.Variables(1).ValuesAsNumpy().tolist(),
            "precipitation_sum":     resp.Variables(2).ValuesAsNumpy().tolist(),
            "sunshine_duration":     resp.Variables(3).ValuesAsNumpy().tolist(),
            "wind_speed_10m_max":    resp.Variables(4).ValuesAsNumpy().tolist(),
            "daylight_duration":     resp.Variables(5).ValuesAsNumpy().tolist(),
        })

        df = transform_df(df)
        city_dfs.append(df)

        buf = StringIO()
        df.to_csv(buf, index=False)
        s3.put_object(
            Bucket=bucket,
            Key=f"weather_backfill/{city_name}.csv",
            Body=buf.getvalue()
        )
        print(f"✅ Uploaded weather_backfill/{city_name}.csv")

    # 2) Concatenate & aggregate all cities
    all_df = pd.concat(city_dfs, ignore_index=True)
    agg = (
        all_df
        .groupby("date", as_index=False)
        .agg({
            "temperature_2m_max": "mean",
            "temperature_2m_min": "mean",
            "temp_mean":          "mean",
            "temp_range":         "mean",
            "precipitation_sum":  "mean",
            "precip_flag":        "max",
            "sunshine_duration":  "mean",
            "wind_speed_10m_max": "mean",
            "daylight_duration":  "mean",
        })
        .sort_values("date")
    )

    agg_buf = StringIO()
    agg.to_csv(agg_buf, index=False)
    s3.put_object(
        Bucket=bucket,
        Key="weather_backfill/aggregated_weather.csv",
        Body=agg_buf.getvalue()
    )
    print("✅ Uploaded weather_backfill/aggregated_weather.csv")

    return {
        "statusCode": 200,
        "body": f"Wrote {len(CITIES)} city files + 1 aggregated file"
    }
