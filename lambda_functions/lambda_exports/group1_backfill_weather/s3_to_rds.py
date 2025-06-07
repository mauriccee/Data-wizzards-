# s3_to_rds.py
import boto3
import psycopg2
from io import StringIO

def sanitize(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name.strip().lower())

def load_all_cities(cities, bucket, pg_config, table_prefix="weather_data"):
    s3   = boto3.client("s3")
    conn = psycopg2.connect(**pg_config)
    cur  = conn.cursor()

    for city in cities:
        safe  = sanitize(city)
        table = f"{table_prefix}_{safe}"
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        cur.execute(f"""
            CREATE TABLE {table} (
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

        key        = f"weather_backfill/{city}.csv"
        local_path = f"/tmp/{city}.csv"
        s3.download_file(bucket, key, local_path)

        with open(local_path, "r") as f:
            cur.copy_expert(f"""
                COPY {table} (
                    date,
                    temperature_2m_max,
                    temperature_2m_min,
                    temp_mean,
                    temp_range,
                    precipitation_sum,
                    precip_flag,
                    sunshine_duration,
                    wind_speed_10m_max,
                    daylight_duration
                ) FROM STDIN WITH CSV HEADER;
            """, f)
        conn.commit()
        print(f"✔ Loaded {city} into {table}")

    cur.close()
    conn.close()

def load_aggregated_weather(bucket, key, pg_config):
    s3   = boto3.client("s3")
    conn = psycopg2.connect(**pg_config)
    cur  = conn.cursor()

    # Recreate summary table
    cur.execute("DROP TABLE IF EXISTS weather_swiss_aggr;")
    cur.execute("""
        CREATE TABLE weather_swiss_aggr (
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

    # Download & COPY the aggregated CSV
    obj  = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read().decode("utf-8")
    f    = StringIO(data)

    cur.copy_expert("""
        COPY weather_swiss_aggr (
            date,
            temperature_2m_max,
            temperature_2m_min,
            temp_mean,
            temp_range,
            precipitation_sum,
            precip_flag,
            sunshine_duration,
            wind_speed_10m_max,
            daylight_duration
        ) FROM STDIN WITH CSV HEADER;
    """, f)
    conn.commit()

    cur.close()
    conn.close()
    print("✔ Loaded aggregated_weather.csv into weather_swiss_aggr")
