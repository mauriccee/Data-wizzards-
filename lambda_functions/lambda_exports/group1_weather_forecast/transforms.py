# transforms.py

import numpy as np
import pandas as pd

def fill_column_na_with_mean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaNs in each numeric column with that column's mean.
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    col_means = df[numeric_cols].mean()
    df[numeric_cols] = df[numeric_cols].fillna(col_means)
    return df

def add_basic_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame with columns:
      - temperature_2m_max, temperature_2m_min,
      - precipitation_sum, sunshine_duration,
      - wind_speed_10m_max, daylight_duration
    compute:
      - temp_mean, temp_range,
      - precip_flag,
    and return a DataFrame with a defined column order.
    """
    df["temp_range"]  = df["temperature_2m_max"] - df["temperature_2m_min"]
    df["temp_mean"]   = (df["temperature_2m_max"] + df["temperature_2m_min"]) / 2
    df["precip_flag"] = (df["precipitation_sum"] > 0).astype(int)

    cols = [
        "date",
        "temperature_2m_max",
        "temperature_2m_min",
        "temp_mean",
        "temp_range",
        "precipitation_sum",
        "precip_flag",
        "sunshine_duration",
        "wind_speed_10m_max",
        "daylight_duration"
    ]
    return df[cols]

def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full pipeline: fill NaNs by column mean, then add features.
    """
    df = fill_column_na_with_mean(df)
    df = add_basic_features(df)
    return df
