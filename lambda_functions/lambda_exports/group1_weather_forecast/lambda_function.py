import os
from datetime import date
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import openmeteo_requests
import requests_cache
from retry_requests import retry

from transforms import transform_df

# ─── Configuration ──────────────────────────────────────────────────────────
CITIES = {
    "Zurich":   (47.3769,  8.5417),
    "Geneva":   (46.2044,  6.1432),
    "Basel":    (47.5596,  7.5886),
    "Lausanne": (46.5197,  6.6323),
    "Bern":     (46.9480,  7.4474),
}
FORECAST_DAYS = 7
TABLE_NAME     = "weather_swiss_forecast_agg"
PG_CONFIG      = {
    "host":     os.environ["PG_HOST"],
    "port":     int(os.environ.get("PG_PORT", 5432)),
    "dbname":   os.environ["PG_DB"],
    "user":     os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}

# ─── Helpers ─────────────────────────────────────────────────────────────────
def get_db_conn():
    return psycopg2.connect(**PG_CONFIG)

def reset_table(cur):
    cur.execute(f"DROP TABLE IF EXISTS {TABLE_NAME};")
    cur.execute(f"""
    CREATE TABLE {TABLE_NAME} (
      date                  DATE     PRIMARY KEY,
      temperature_2m_max    FLOAT,
      temperature_2m_min    FLOAT,
      temp_mean             FLOAT,
      temp_range            FLOAT,
      precipitation_sum     FLOAT,
      precip_flag           INT,
      sunshine_duration     FLOAT,
      wind_speed_10m_max    FLOAT,
      daylight_duration     FLOAT,
      days_ahead            INT
    );
    """)

# ─── Fetch & Transform ───────────────────────────────────────────────────────
def fetch_and_transform_forecast(lat, lon):
    sess   = requests_cache.CachedSession('/tmp/.cache_fc', expire_after=3600)
    client = openmeteo_requests.Client(session=retry(sess, retries=5, backoff_factor=0.2))
    resp   = client.weather_api(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude":      lat,
            "longitude":     lon,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "sunshine_duration",
                "wind_speed_10m_max",
                "daylight_duration"
            ],
            "forecast_days": FORECAST_DAYS,
            "timezone":      "UTC"
        }
    )[0].Daily()

    # build date range using Time, TimeEnd, Interval
    start = pd.to_datetime(resp.Time(), unit="s", utc=True)
    end   = pd.to_datetime(resp.TimeEnd(), unit="s", utc=True)
    freq  = pd.Timedelta(seconds=resp.Interval())
    dates = pd.date_range(start=start, end=end, freq=freq, inclusive="left").date

    df = pd.DataFrame({
        "date":                dates,
        "temperature_2m_max":  resp.Variables(0).ValuesAsNumpy().tolist(),
        "temperature_2m_min":  resp.Variables(1).ValuesAsNumpy().tolist(),
        "precipitation_sum":   resp.Variables(2).ValuesAsNumpy().tolist(),
        "sunshine_duration":   resp.Variables(3).ValuesAsNumpy().tolist(),
        "wind_speed_10m_max":  resp.Variables(4).ValuesAsNumpy().tolist(),
        "daylight_duration":   resp.Variables(5).ValuesAsNumpy().tolist(),
    })
    # apply shared transform logic
    return transform_df(df)

# ─── Lambda Handler ────────────────────────────────────────────────────────── ──────────────────────────────────────────────────────────
def lambda_handler(event, context):
    # Fetch forecasts for all cities
    dfs = [fetch_and_transform_forecast(lat, lon) for lat, lon in CITIES.values()]
    combined = pd.concat(dfs, ignore_index=True)

    # Ensure date is datetime64
    combined["date"] = pd.to_datetime(combined["date"])

    # Aggregate across cities
    aggr = (
        combined
        .groupby("date", as_index=False, sort=True)
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
    )

    # Compute days ahead
    today = date.today()
    aggr["days_ahead"] = (aggr["date"].dt.date - today).apply(lambda d: d.days)

    # Write to DB
    conn = get_db_conn()
    cur  = conn.cursor()
    reset_table(cur)

    cols = [
        "date", "temperature_2m_max", "temperature_2m_min",
        "temp_mean", "temp_range", "precipitation_sum",
        "precip_flag", "sunshine_duration",
        "wind_speed_10m_max", "daylight_duration",
        "days_ahead"
    ]
    values = list(aggr[cols].itertuples(index=False, name=None))
    insert_sql = f"INSERT INTO {TABLE_NAME} ({', '.join(cols)}) VALUES %s"
    execute_values(cur, insert_sql, values)

    conn.commit()
    cur.close()
    conn.close()

    return {"statusCode": 200, "body": f"Stored {len(aggr)} forecast rows in {TABLE_NAME}"}
