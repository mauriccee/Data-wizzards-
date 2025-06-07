#------archive.py

import os
from datetime import date, timedelta

import openmeteo_requests
import requests_cache
from retry_requests import retry
import psycopg2

# ─── 1) Swiss cities and sanitization ────────────────────────────────────────
CITIES = {
    "Zurich":   (47.3769,  8.5417),
    "Geneva":   (46.2044,  6.1432),
    "Basel":    (47.5596,  7.5886),
    "Lausanne": (46.5197,  6.6323),
    "Bern":     (46.9480,  7.4474),
}

def sanitize(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name.lower())

# ─── 2) Archive lag & “last available” ─────────────────────────────────────
ARCHIVE_LAG = timedelta(days=5)
today       = date.today()
last_avail  = today - ARCHIVE_LAG  # newest date the archive API can return

# ─── 3) Open-Meteo client w/ cache & retry ──────────────────────────────────
_cache   = requests_cache.CachedSession('/tmp/.cache', expire_after=3600)
_session = retry(_cache, retries=5, backoff_factor=0.2)
om       = openmeteo_requests.Client(session=_session)

# ─── 4) RDS config ──────────────────────────────────────────────────────────
PG_CONFIG = {
    "host":     os.environ["PG_HOST"],
    "port":     int(os.environ.get("PG_PORT", 5432)),
    "dbname":   os.environ["PG_DB"],
    "user":     os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}

def lambda_handler(event, context):
    conn = psycopg2.connect(**PG_CONFIG)
    cur  = conn.cursor()

    for city, (lat, lon) in CITIES.items():
        table = f"weather_data_{sanitize(city)}"

        # ─── Ensure schema matches backfill ───────────────────────────────
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
          date                DATE     PRIMARY KEY,
          temperature_2m_max  FLOAT,
          temperature_2m_min  FLOAT,
          temp_mean           FLOAT,
          temp_range          FLOAT,
          precipitation_sum   FLOAT,
          precip_flag         INT,
          sunshine_duration   FLOAT,
          wind_speed_10m_max  FLOAT,
          daylight_duration   FLOAT
        );
        """)
        conn.commit()

        # ─── Find last stored date ────────────────────────────────────────
        cur.execute(f"SELECT MAX(date) FROM {table};")
        last_stored = cur.fetchone()[0]
        if last_stored is None:
            print(f"{city}: no existing data, skipping daily update")
            continue

        next_date = last_stored + timedelta(days=1)
        if next_date > last_avail:
            print(f"{city}: up to date (last {last_stored}, archive to {last_avail})")
            continue

        # ─── Fetch exactly one day of raw data ──────────────────────────
        resp = om.weather_api(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude":   lat,
                "longitude":  lon,
                "start_date": next_date.isoformat(),
                "end_date":   next_date.isoformat(),
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

        # ─── Extract the one value per variable ─────────────────────────
        # indices: 0=max, 1=min, 2=precip, 3=sunshine, 4=wind, 5=daylight
        def get_val(idx):
            return resp.Variables(idx).ValuesAsNumpy().tolist()[0]

        t_max    = get_val(0)
        t_min    = get_val(1)
        precip   = get_val(2)
        sunshine = get_val(3)
        wind     = get_val(4)
        light    = get_val(5)

        # ─── Compute derived features ────────────────────────────────────
        temp_mean   = (t_max + t_min) / 2
        temp_range  = t_max  - t_min
        precip_flag = int(precip > 0)

        # ─── Upsert into the 10-column table ─────────────────────────────
        upsert_sql = f"""
        INSERT INTO {table} (
          date,
          temperature_2m_max, temperature_2m_min,
          temp_mean, temp_range,
          precipitation_sum, precip_flag,
          sunshine_duration,
          wind_speed_10m_max, daylight_duration
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
          temperature_2m_max  = EXCLUDED.temperature_2m_max,
          temperature_2m_min  = EXCLUDED.temperature_2m_min,
          temp_mean           = EXCLUDED.temp_mean,
          temp_range          = EXCLUDED.temp_range,
          precipitation_sum   = EXCLUDED.precipitation_sum,
          precip_flag         = EXCLUDED.precip_flag,
          sunshine_duration   = EXCLUDED.sunshine_duration,
          wind_speed_10m_max  = EXCLUDED.wind_speed_10m_max,
          daylight_duration   = EXCLUDED.daylight_duration;
        """
        cur.execute(upsert_sql, [
            next_date,
            t_max, t_min,
            temp_mean, temp_range,
            precip, precip_flag,
            sunshine,
            wind, light
        ])
        conn.commit()

        print(f"{city}: inserted/updated {next_date}")

    cur.close()
    conn.close()

    return {"statusCode": 200, "body": "Daily archive update complete"}
