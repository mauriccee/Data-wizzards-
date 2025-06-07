import sys, types
# ==== numpy.testing stub hack (if needed) ====
import numpy as _np
_testing = types.ModuleType("numpy.testing")
_utils = types.ModuleType("numpy.testing.utils")
_np.testing = _testing
_testing.utils = _utils
sys.modules["numpy.testing"] = _testing
sys.modules["numpy.testing.utils"] = _utils
# ==== end stub ====
import os
import io
import pickle
import boto3
import pandas as pd
import psycopg2
import holidays
from datetime import datetime

# ─── Configuration ───────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET"]
PG_CONFIG = {
    "host":     os.environ["PG_HOST"],
    "port":     int(os.environ.get("PG_PORT", 5432)),
    "dbname":   os.environ["PG_DB"],
    "user":     os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}
MODEL_S3_PREFIX = "modelling/ols_model_"
HIST_DAYS = 8
s3 = boto3.client("s3")


def lambda_handler(event, context):
    print("Lambda start: loading historical data")
    conn = psycopg2.connect(**PG_CONFIG)
    energy_df = pd.read_sql_query(
        "SELECT timestamp, load_mw FROM energy_data",
        conn, parse_dates=["timestamp"]
    )
    conn.close()
    print(f"Loaded {len(energy_df)} historical rows")

    energy_df["timestamp"] = pd.to_datetime(energy_df["timestamp"], utc=True)
    energy_df = energy_df.set_index("timestamp").sort_index()
    energy_df = energy_df[~energy_df.index.duplicated(keep="first")]

    full_idx = pd.date_range(
        start=energy_df.index.max() - pd.Timedelta(days=HIST_DAYS),
        end=energy_df.index.max(),
        freq="h",
        tz="UTC"
    )
    energy_df = energy_df.reindex(full_idx)

    energy_df["load_mw"] = (
        energy_df["load_mw"]
        .interpolate(limit=3)
        .ffill()
        .bfill()
    )

    energy_df = energy_df.reset_index().rename(columns={"index": "timestamp"})
    load_map = dict(zip(energy_df["timestamp"], energy_df["load_mw"]))

    print("Loading weather forecast data")
    conn = psycopg2.connect(**PG_CONFIG)
    weather_df = pd.read_sql_query(
        """
        SELECT date, temp_mean, temp_range, precipitation_sum,
               sunshine_duration, wind_speed_10m_max, daylight_duration, days_ahead
        FROM weather_swiss_forecast_agg
        WHERE days_ahead BETWEEN 0 AND 7
        ORDER BY days_ahead
        """,
        conn, parse_dates=["date"]
    )
    conn.close()
    print(f"Loaded forecast dates: {weather_df['date'].dt.date.tolist()}")
    weather_df["date"] = weather_df["date"].dt.tz_localize("UTC")

    print("Building hourly skeleton for next 7 days")
    rows = []
    for date in weather_df["date"]:
        for hr in range(24):
            rows.append({"timestamp": date + pd.Timedelta(hours=hr), "date": date, "hour": hr})
    df = pd.DataFrame(rows)
    print(f"Skeleton rows: {len(df)}")

    print("Merging weather features")
    df = df.merge(weather_df, on="date", how="left")
    print(f"After weather merge, nulls in temp_mean: {df['temp_mean'].isna().sum()}")

    print("Adding calendar features")
    swiss_hols = holidays.Switzerland(years=weather_df["date"].dt.year.unique())
    df["is_holiday"] = df["date"].dt.date.isin(swiss_hols)
    df["wday"] = df["timestamp"].dt.day_name()
    df["hour"] = df["hour"].astype('category')
    df["wday"] = df["wday"].astype('category')

    print("Loading model artifact from S3")
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=MODEL_S3_PREFIX)
    keys = [o['Key'] for o in resp.get('Contents', []) if o['Key'].endswith('.pkl')]
    latest = sorted(keys)[-1]
    print(f"Using model key: {latest}")
    model_blob = s3.get_object(Bucket=S3_BUCKET, Key=latest)['Body'].read()
    model = pickle.loads(model_blob)

    print("Recursive forecasting for 7 days")
    predicted_loads = {}
    forecast_results = []
    df = df.sort_values("timestamp")
    timestamps = df["timestamp"].sort_values().unique()

    for t in timestamps:
        row = df[df["timestamp"] == t].copy()
        t_lag24 = t - pd.Timedelta(hours=24)
        t_lag168 = t - pd.Timedelta(hours=168)

        lag24 = predicted_loads.get(t_lag24, load_map.get(t_lag24))
        lag168 = predicted_loads.get(t_lag168, load_map.get(t_lag168))

        row.loc[:, "lag24"] = lag24
        row.loc[:, "lag168"] = lag168

        if pd.isna(row["lag24"].values[0]) or pd.isna(row["lag168"].values[0]):
            continue
        
        pred = model.predict(row).iloc[0]

        row.loc[:, "predicted_load_mw"] = pred

        predicted_loads[t] = pred
        forecast_results.append(row)

    df = pd.concat(forecast_results)
    print(f"Predicted rows: {len(df)}")

    print("Filtering and saving results")
    result_df = df[["timestamp", "predicted_load_mw"]].copy()
    out = io.StringIO()
    result_df.to_csv(out, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=f"modelling/forecast_predictions_{datetime.utcnow():%Y%m%d_%H%M%S}.csv", Body=out.getvalue())

    print("Upserting into RDS")
    conn2 = psycopg2.connect(**PG_CONFIG)
    cur2 = conn2.cursor()
    cur2.execute("""
        CREATE TABLE IF NOT EXISTS energy_forecast (
            timestamp        TIMESTAMP PRIMARY KEY,
            predicted_load_mw FLOAT
        );
    """)
    conn2.commit()

    rows = result_df.values.tolist()
    values_str = ",".join(
        cur2.mogrify("(%s,%s)", row).decode() for row in rows
    )
    upsert_sql = f"""
        INSERT INTO energy_forecast (timestamp, predicted_load_mw)
        VALUES {values_str}
        ON CONFLICT (timestamp) DO UPDATE
          SET predicted_load_mw = EXCLUDED.predicted_load_mw;
    """
    cur2.execute(upsert_sql)
    conn2.commit()
    cur2.close()
    conn2.close()

    print("Lambda completed successfully with upsert")
    return {
        "status":  "success",
        "message": "Forecast generated and saved (S3 + RDS with upsert)"
    }
