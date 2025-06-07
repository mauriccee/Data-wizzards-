import numpy as np
import pandas as pd

def fill_column_na_with_mean(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    print(" ▶ before fill, NA counts:\n", df[numeric_cols].isna().sum())
    col_means = df[numeric_cols].mean()
    print(" ▶ column‐means:\n", col_means.to_dict())
    df[numeric_cols] = df[numeric_cols].fillna(col_means)
    print(" ▶ after fill, NA counts:\n", df[numeric_cols].isna().sum())
    return df


def add_basic_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Given a DataFrame with daily weather columns, compute derived features:
      - temp_mean = (max + min) / 2
      - temp_range = max - min
      - precip_flag = 1 if precipitation_sum > 0 else 0
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
    Full pipeline: fill missing values and add basic features.
    """
    df = fill_column_na_with_mean(df)
    df = add_basic_features(df)
    return df
