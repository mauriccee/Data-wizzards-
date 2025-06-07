import numpy as np
import pandas as pd

def fill_na_last_5_days(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each numeric column, fill NaNs with the rolling mean over the
    previous 5 days (including today). Assumes df has a 'date' column.
    """
    # Make sure rows are in chronological order
    df = df.sort_values("date").reset_index(drop=True)

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    # Compute a 5-day rolling mean (min_periods=1 so we always get a value)
    rolling_means = df[numeric_cols].rolling(window=5, min_periods=1).mean()
    # Fill NaNs in-place
    df[numeric_cols] = df[numeric_cols].fillna(rolling_means)
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
        "daylight_duration",
    ]
    return df[cols]

def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Full pipeline: fill NaNs with last-5-day mean, then add basic features.
    """
    df = fill_na_last_5_days(df)
    df = add_basic_features(df)
    return df
