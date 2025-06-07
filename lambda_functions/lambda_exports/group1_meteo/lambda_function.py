#====== lambda_function.py ======
from datetime import date, timedelta
import db, config
from fetcher_range import fetch_historical_range
from upsert import upsert_record, upsert_aggregate

def lambda_handler(event, context):
    conn = db.get_connection()
    cur  = conn.cursor()

    # ensure per-city and aggregate tables exist
    for city in config.CITIES:
        db.ensure_city_table(cur, city)
    db.ensure_agg_table(cur)
    conn.commit()

    updated_dates = []
    today = date.today()

        # For each city, always pull the last ARCHIVE_DAYS of data
    for city, (lat, lon) in config.CITIES.items():
        # define a rolling window [today - ARCHIVE_DAYS … today - 1]
        start = today - timedelta(days=config.ARCHIVE_DAYS)
        end   = today - timedelta(days=1)

        # fetch & upsert that window every run
        df = fetch_historical_range(lat, lon, start.isoformat(), end.isoformat())
        for row in df.itertuples(index=False, name=None):
            data = dict(zip(df.columns, row))
            upsert_record(cur, city, data)
            updated_dates.append(data['date'])
        conn.commit()


    # Aggregate for all updated dates
    for d in sorted(set(updated_dates)):
        upsert_aggregate(cur, d)
    conn.commit()

    cur.close()
    conn.close()
    return {"statusCode":200, "body": f"Updated {len(set(updated_dates))} days across {len(config.CITIES)} cities."}
