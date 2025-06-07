# upsert.py
import psycopg2
from psycopg2 import sql
import config
import db
from psycopg2.extras import execute_values

def upsert_record(cur, city: str, data: dict):
    """
    Upsert a single row into the per-city weather table.
    """
    table = f"weather_data_{db.sanitize(city)}"
    cols = list(data.keys())
    vals = [data[c] for c in cols]
    placeholders = ",".join(["%s"] * len(cols))
    update_sets = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c != 'date'])

    insert_sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) " \
                 f"ON CONFLICT (date) DO UPDATE SET {update_sets};"
    cur.execute(insert_sql, vals)


def upsert_aggregate(cur, target_date):
    """
    Aggregate all city tables for the given date and upsert into the Swiss aggregate table.
    """
    # Build UNION ALL of per-city selects
    union_parts = []
    params = []
    for city in config.CITIES:
        tbl = f"weather_data_{db.sanitize(city)}"
        union_parts.append(f"SELECT * FROM {tbl} WHERE date = %s")
        params.append(target_date)
    union_sql = " UNION ALL ".join(union_parts)

    # Build full aggregation SQL
    agg_sql = f"""
INSERT INTO {config.AGG_TABLE} (
  date,
  temperature_2m_max, temperature_2m_min,
  temp_mean, temp_range,
  precipitation_sum, precip_flag,
  sunshine_duration, wind_speed_10m_max, daylight_duration
)
SELECT
  date,
  AVG(temperature_2m_max)      AS temperature_2m_max,
  AVG(temperature_2m_min)      AS temperature_2m_min,
  AVG(temp_mean)               AS temp_mean,
  AVG(temp_range)              AS temp_range,
  AVG(precipitation_sum)       AS precipitation_sum,
  MAX(precip_flag)::INT        AS precip_flag,
  AVG(sunshine_duration)       AS sunshine_duration,
  AVG(wind_speed_10m_max)      AS wind_speed_10m_max,
  AVG(daylight_duration)       AS daylight_duration
FROM (
  {union_sql}
) sub
GROUP BY date
ON CONFLICT (date) DO UPDATE SET
  temperature_2m_max    = EXCLUDED.temperature_2m_max,
  temperature_2m_min    = EXCLUDED.temperature_2m_min,
  temp_mean             = EXCLUDED.temp_mean,
  temp_range            = EXCLUDED.temp_range,
  precipitation_sum     = EXCLUDED.precipitation_sum,
  precip_flag           = EXCLUDED.precip_flag,
  sunshine_duration     = EXCLUDED.sunshine_duration,
  wind_speed_10m_max    = EXCLUDED.wind_speed_10m_max,
  daylight_duration     = EXCLUDED.daylight_duration;
"""
    cur.execute(agg_sql, params)
