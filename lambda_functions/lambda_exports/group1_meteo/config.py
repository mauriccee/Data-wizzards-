#====== config.py ======
import os
from datetime import timedelta

# PostgreSQL connection settings
PG_CONFIG = {
    "host":     os.environ["PG_HOST"],
    "port":     int(os.environ.get("PG_PORT", 5432)),
    "dbname":   os.environ["PG_DB"],
    "user":     os.environ["PG_USER"],
    "password": os.environ["PG_PASSWORD"],
}

# Swiss cities
CITIES = {
    "Zurich":   (47.3769,  8.5417),
    "Geneva":   (46.2044,  6.1432),
    "Basel":    (47.5596,  7.5886),
    "Lausanne": (46.5197,  6.6323),
    "Bern":     (46.9480,  7.4474),
}

# How many days of history to refresh each run
ARCHIVE_DAYS = 3

# Open-Meteo archive endpoint
ARCHIVE_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"

# Cache settings
CACHE_PATH = "/tmp/.cache"

# Aggregate table name
AGG_TABLE = "weather_swiss_aggr"
