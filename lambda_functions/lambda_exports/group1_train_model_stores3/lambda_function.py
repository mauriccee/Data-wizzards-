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

import boto3
import pandas as pd
import io
import holidays
import statsmodels.formula.api as smf
import pickle
from datetime import datetime

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    # S3 Bucket paths
    weather_bucket = 'group1-storage-weather'
    energy_bucket = 'group1-storage-weather'

    weather_prefix = 'weather_backfill/aggregated_weather'
    energy_key = 'energy_backup/energy_data.csv'

    # Fetch energy data
    energy_obj = s3.get_object(Bucket=energy_bucket, Key=energy_key)
    energy_gen = pd.read_csv(io.BytesIO(energy_obj['Body'].read()),
                             usecols=["timestamp", "gen_type", "quantity_mw", "load_mw", "price_eur_per_mwh"])

    energy_gen["timestamp"] = pd.to_datetime(energy_gen["timestamp"], utc=True)
    energy_gen["date"] = energy_gen["timestamp"].dt.normalize().dt.tz_convert(None)

    gen_pivot = energy_gen.pivot_table(
        index=["timestamp", "date"],
        columns="gen_type",
        values="quantity_mw",
        aggfunc="sum"
    ).reset_index()

    load_price = (
        energy_gen.groupby("timestamp")[["load_mw", "price_eur_per_mwh"]]
        .first().reset_index()
    )
    energy = pd.merge(gen_pivot, load_price, on="timestamp", how="left")

    energy["hour"] = energy["timestamp"].dt.hour.astype("category")
    energy["wday"] = energy["timestamp"].dt.day_name().astype("category")

    us_hols = holidays.US(years=[2022, 2023, 2024, 2025])
    holiday_dates = pd.to_datetime(list(us_hols.keys())).normalize()
    energy["is_holiday"] = energy["date"].isin(holiday_dates)

    # Fetch & aggregate weather data (all CSVs from prefix)
    weather_objs = s3.list_objects_v2(Bucket=weather_bucket, Prefix=weather_prefix)
    weather_dfs = []
    for obj in weather_objs.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            data = s3.get_object(Bucket=weather_bucket, Key=obj['Key'])
            df = pd.read_csv(io.BytesIO(data['Body'].read()), parse_dates=["date"])
            weather_dfs.append(df)

    weather = pd.concat(weather_dfs, ignore_index=True)

    weather["date"] = weather["date"].dt.normalize()
    weather["temp_mean"] = (weather["temperature_2m_max"] + weather["temperature_2m_min"]) / 2
    weather["temp_range"] = (weather["temperature_2m_max"] - weather["temperature_2m_min"])

    df3 = pd.merge(energy, weather, on="date", how="inner")
    df3 = df3.sort_values("timestamp").reset_index(drop=True)
    df3["lag24"] = df3["load_mw"].shift(24)
    df3["lag168"] = df3["load_mw"].shift(168)
    df3 = df3.dropna(subset=["lag168"]).reset_index(drop=True)

    # Train the model
    formula = (
        "load_mw ~ lag24 + lag168 + C(hour) + C(wday) + is_holiday + "
        "temp_mean + temp_range + precipitation_sum + sunshine_duration + "
        "wind_speed_10m_max + daylight_duration"
    )
    model_sub = smf.ols(formula=formula, data=df3).fit()

    # Save predictions
    predictions = model_sub.predict(df3)
    prediction_df = df3[['timestamp']].copy()
    prediction_df['predicted_load_mw'] = predictions

    pred_csv_buffer = io.StringIO()
    prediction_df.to_csv(pred_csv_buffer, index=False)
    s3.put_object(
        Bucket=weather_bucket,
        Key=f'modelling/predictions_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.csv',
        Body=pred_csv_buffer.getvalue()
    )

    # Save model artifact with pickle
    model_buffer = io.BytesIO()
    pickle.dump(model_sub, model_buffer)
    model_buffer.seek(0)
    s3.put_object(
        Bucket=weather_bucket,
        Key=f'modelling/ols_model_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.pkl',
        Body=model_buffer.getvalue()
    )

    return {'status': 'success', 'message': 'Model trained and artifacts saved.'}
