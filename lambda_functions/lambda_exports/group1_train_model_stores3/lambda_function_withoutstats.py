import boto3
import csv
import io
import pickle
import numpy as np
from datetime import datetime, timezone

def parse_timestamp(ts_str):
    """Parse '2022-01-03 01:00:00.000 +0100' into a UTC-naive datetime."""
    try:
        # Pure ISO (if you ever have one)
        return datetime.fromisoformat(ts_str)
    except ValueError:
        # Your actual format:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f %z")
        # Convert to UTC and drop tzinfo
        return dt.astimezone(timezone.utc).replace(tzinfo=None)

def read_csv_from_s3(s3, bucket, key, cols):
    obj = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        yield {c: row[c] for c in cols}

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    bucket = "group1-storage-weather"
    energy_key = "energy_backup/energy_data.csv"
    weather_key = "weather_backfill/aggregated_weather.csv"

    # --- 1) Read & pivot energy data ---
    pivot = {}       # (ts, date) → {gen_type: sum}
    load_price = {}  # ts → (load, price)
    for r in read_csv_from_s3(s3, bucket, energy_key,
                              ["timestamp","gen_type","quantity_mw","load_mw","price_eur_per_mwh"]):
        ts = parse_timestamp(r["timestamp"])
        d  = ts.date()
        pivot.setdefault((ts,d), {}).setdefault(r["gen_type"], 0.0)
        pivot[(ts,d)][r["gen_type"]] += float(r["quantity_mw"])
        load_price[ts] = (
            float(r["load_mw"]),
            float(r["price_eur_per_mwh"])
        )

    energy_rows = []
    for (ts,d), gen_map in pivot.items():
        load, price = load_price[ts]
        row = {
            "timestamp": ts,
            "date":      d,
            "load_mw":   load,
            "price_eur": price,
        }
        row.update(gen_map)
        row["hour"] = ts.hour
        row["wday"] = ts.weekday()  # Mon=0…Sun=6
        energy_rows.append(row)

    # --- 2) Read Swiss weather data (only date, temp_mean, temp_range) ---
    weather_map = {}  # date → dict(temp_mean, temp_range)
    obj = s3.get_object(Bucket=bucket, Key=weather_key)
    text = obj["Body"].read().decode("utf-8")
    rdr  = csv.DictReader(io.StringIO(text))
    for r in rdr:
        d = datetime.fromisoformat(r["date"]).date()
        weather_map[d] = {
            "temp_mean":  float(r["temp_mean"]),
            "temp_range": float(r["temp_range"]),
        }

    # --- 3) Join & sort, then build lag features ---
    combined = []
    for row in energy_rows:
        w = weather_map.get(row["date"])
        if not w:
            continue
        combined.append({**row, **w})
    combined.sort(key=lambda r: r["timestamp"])

    model_rows = []
    for i, r in enumerate(combined):
        if i < 168:
            continue
        r["lag24"]  = combined[i-24]["load_mw"]
        r["lag168"] = combined[i-168]["load_mw"]
        model_rows.append(r)

    # --- 4) Build design matrix & target vector (4 numeric + 24 hr + 7 wday = 35) ---
    N = len(model_rows)
    F = 4 + 24 + 7
    X = np.zeros((N, F), dtype=float)
    y = np.zeros(N, dtype=float)

    for i, r in enumerate(model_rows):
        # numeric
        X[i, 0] = r["lag24"]
        X[i, 1] = r["lag168"]
        X[i, 2] = r["temp_mean"]
        X[i, 3] = r["temp_range"]
        # hour one-hot at cols 4..27
        X[i, 4 + r["hour"]] = 1.0
        # wday one-hot at cols 28..34
        X[i, 4 + 24 + r["wday"]] = 1.0
        y[i] = r["load_mw"]

    
    N, F = X.shape
    if N <= F:
        msg = f"Not enough data to fit model: N={N}, F={F}"
        print(msg)
        return {"status":"error", "message": msg}

    # Attempt a least-squares solve, with pinv fallback
    try:
        β, *_ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError as e:
        print("lstsq failed, falling back to pinv:", e)
        β = np.linalg.pinv(X).dot(y)

    preds = X.dot(β)

       # --- 5) Solve OLS & predict ---
    try:
        β, *_ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        β = np.linalg.pinv(X).dot(y)
    preds = X.dot(β)

    # --- Compute and print R-squared ---
    residuals = y - preds
    ss_res = np.sum(residuals ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot
    print(f"R-squared: {r2:.4f}")

    # --- 6) Save predictions ---
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["timestamp", "predicted_load_mw"])
    for r, p in zip(model_rows, preds):
        writer.writerow([r["timestamp"].isoformat(), p])
    s3.put_object(
        Bucket=bucket,
        Key=f"modelling/predictions_{datetime.utcnow():%Y%m%d_%H%M%S}.csv",
        Body=out.getvalue()
    )

    # --- 7) Save model artifact ---
    feature_names = (
        ["lag24","lag168","temp_mean","temp_range"]
        + [f"hour_{h}" for h in range(24)]
        + [f"wday_{d}"  for d in range(7)]
    )
    artifact = {"coef": β.tolist(), "features": feature_names}
    buf = io.BytesIO()
    pickle.dump(artifact, buf)
    buf.seek(0)
    s3.put_object(
        Bucket=bucket,
        Key=f"modelling/ols_model_{datetime.utcnow():%Y%m%d_%H%M%S}.pkl",
        Body=buf.getvalue()
    )

    return {"status": "success", "message": "Model run and artifacts saved."}
