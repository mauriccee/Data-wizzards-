import pandas as pd
import requests_cache
import openmeteo_requests
from retry_requests import retry
import config
from transforms import transform_df

def fetch_historical_range(lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame:
    # Setup a cached session with retry logic
    sess   = requests_cache.CachedSession(config.CACHE_PATH, expire_after=3600)
    client = openmeteo_requests.Client(session=retry(sess, retries=5, backoff_factor=0.2))

    # Fetch full-precision daily archive via FlatBuffers
    resp = client.weather_api(
        config.ARCHIVE_ENDPOINT,
        params={
            "latitude":   lat,
            "longitude":  lon,
            "start_date": start_date,
            "end_date":   end_date,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "sunshine_duration",
                "wind_speed_10m_max",
                "daylight_duration"
            ],
        }
    )[0].Daily()

    # Build date index (convert to .date for simplicity)
    times = pd.date_range(
        start=pd.to_datetime(resp.Time(),    unit="s", utc=True),
        end=  pd.to_datetime(resp.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=resp.Interval()),
        inclusive="left",
    ).date

    # Construct DataFrame using the same index order as in your backfill script
    df = pd.DataFrame({
        "date":                 times,
        "temperature_2m_max":   resp.Variables(0).ValuesAsNumpy().tolist(),
        "temperature_2m_min":   resp.Variables(1).ValuesAsNumpy().tolist(),
        "precipitation_sum":    resp.Variables(2).ValuesAsNumpy().tolist(),
        "sunshine_duration":    resp.Variables(3).ValuesAsNumpy().tolist(),
        "wind_speed_10m_max":   resp.Variables(4).ValuesAsNumpy().tolist(),
        "daylight_duration":    resp.Variables(5).ValuesAsNumpy().tolist(),
    })

    # Apply your transformation pipeline (NA-fill, feature computation)
    return transform_df(df)
