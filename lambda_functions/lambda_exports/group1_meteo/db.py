#====== db.py ======
import psycopg2
from psycopg2 import sql
import config

def get_connection():
    return psycopg2.connect(**config.PG_CONFIG)

def sanitize(name: str) -> str:
    return ''.join(c if c.isalnum() else '_' for c in name.lower())

def ensure_city_table(cur, city: str):
    table = f"weather_data_{sanitize(city)}"
    cur.execute(sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} ("
        "date DATE PRIMARY KEY,"
        "temperature_2m_max FLOAT, temperature_2m_min FLOAT,"
        "temp_mean FLOAT, temp_range FLOAT,"
        "precipitation_sum FLOAT, precip_flag INT,"
        "sunshine_duration FLOAT, wind_speed_10m_max FLOAT,"
        "daylight_duration FLOAT"
        ");"
    ).format(sql.Identifier(table)))


def ensure_agg_table(cur):
    cur.execute(sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} ("
        "date DATE PRIMARY KEY,"
        "temperature_2m_max FLOAT, temperature_2m_min FLOAT,"
        "temp_mean FLOAT, temp_range FLOAT,"
        "precipitation_sum FLOAT, precip_flag INT,"
        "sunshine_duration FLOAT, wind_speed_10m_max FLOAT,"
        "daylight_duration FLOAT"
        ");"
    ).format(sql.Identifier(config.AGG_TABLE)))