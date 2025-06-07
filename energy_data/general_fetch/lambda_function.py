import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import isodate
import psycopg2
import os
API_KEY = os.environ['API_KEY']
BASE_URL = "https://web-api.tp.entsoe.eu/api"

GEN_TYPE_LABELS = {
    "B01": "biomass",
    "B02": "fossil_brown_coal_lignite",
    "B03": "fossil_coal_derived_gas",
    "B04": "fossil_gas",
    "B05": "fossil_hard_coal",
    "B06": "fossil_oil",
    "B07": "fossil_oil_shale",
    "B08": "fossil_peat",
    "B09": "geothermal",
    "B10": "hydro_pumped_storage",
    "B11": "hydro_run_of_river_and_poundage",
    "B12": "hydro_water_reservoir",
    "B13": "marine",
    "B14": "nuclear",
    "B15": "other_renewable",
    "B16": "solar",
    "B17": "waste",
    "B18": "wind_offshore",
    "B19": "wind_onshore",
    "B20": "other",
    "B25": "energy_storage"
}

# ───────────── Fetch Functions ─────────────

def fetch_generation_per_type(start, end):
    params = {
        'documentType': 'A75',
        'processType': 'A16',
        'outBiddingZone_Domain': '10YCH-SWISSGRIDZ',
        'periodStart': start.strftime('%Y%m%d%H%M'),
        'periodEnd': end.strftime('%Y%m%d%H%M'),
        'in_Domain': '10YCH-SWISSGRIDZ',
        'securityToken': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    root = ET.fromstring(response.text)
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
    data = []
    for ts in root.findall("ns:TimeSeries", ns):
        psr_type = ts.find("ns:MktPSRType/ns:psrType", ns)
        if psr_type is None:
            continue
        gen_type = psr_type.text
        period = ts.find("ns:Period", ns)
        start_time = isodate.parse_datetime(period.find("ns:timeInterval/ns:start", ns).text)
        resolution = isodate.parse_duration(period.find("ns:resolution", ns).text)
        for point in period.findall("ns:Point", ns):
            pos = int(point.find("ns:position", ns).text)
            qty = float(point.find("ns:quantity", ns).text)
            timestamp = start_time + resolution * (pos - 1)
            data.append((timestamp, gen_type, qty))
    return pd.DataFrame(data, columns=["timestamp", "gen_type", "quantity_mw"])

def fetch_monthly_load(start, end):
    params = {
        'documentType': 'A65',
        'processType': 'A16',
        'outBiddingZone_Domain': '10YCH-SWISSGRIDZ',
        'periodStart': start.strftime('%Y%m%d%H%M'),
        'periodEnd': end.strftime('%Y%m%d%H%M'),
        'securityToken': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    root = ET.fromstring(response.text)
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
    data = []
    for ts in root.findall("ns:TimeSeries", ns):
        period = ts.find("ns:Period", ns)
        start_time = isodate.parse_datetime(period.find("ns:timeInterval/ns:start", ns).text)
        resolution = isodate.parse_duration(period.find("ns:resolution", ns).text)
        for point in period.findall("ns:Point", ns):
            pos = int(point.find("ns:position", ns).text)
            qty = float(point.find("ns:quantity", ns).text)
            timestamp = start_time + resolution * (pos - 1)
            data.append((timestamp, qty))
    return pd.DataFrame(data, columns=["timestamp", "load_mw"])

def fetch_energy_prices(start, end):
    params = {
        'documentType': 'A44',
        'in_Domain': '10YCH-SWISSGRIDZ',
        'out_Domain': '10YCH-SWISSGRIDZ',
        'periodStart': start.strftime('%Y%m%d%H%M'),
        'periodEnd': end.strftime('%Y%m%d%H%M'),
        'securityToken': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    ns = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}
    root = ET.fromstring(response.text)
    data = []
    for ts in root.findall("ns:TimeSeries", ns):
        period = ts.find("ns:Period", ns)
        start_time = isodate.parse_datetime(period.find("ns:timeInterval/ns:start", ns).text)
        resolution = isodate.parse_duration(period.find("ns:resolution", ns).text)
        for point in period.findall("ns:Point", ns):
            pos = int(point.find("ns:position", ns).text)
            price = float(point.find("ns:price.amount", ns).text)
            timestamp = start_time + resolution * (pos - 1)
            data.append((timestamp, price))
    return pd.DataFrame(data, columns=["timestamp", "price_eur_per_mwh"])

# ───────────── Lambda Entry Point ─────────────

def lambda_handler(event, context):
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2025, 1, 1)

    all_gen, all_load, all_price = [], [], []

    while start_date < end_date:
        next_date = start_date + timedelta(days=1)
        print(f"Fetching data for {start_date.strftime('%Y-%m')}")

        try:
            all_gen.append(fetch_generation_per_type(start_date, next_date))
        except Exception as e:
            print(f"Generation error for {start_date}: {e}")

        try:
            all_load.append(fetch_monthly_load(start_date, next_date))
        except Exception as e:
            print(f"Load error for {start_date}: {e}")

        try:
            all_price.append(fetch_energy_prices(start_date, next_date))
        except Exception as e:
            print(f"Price error for {start_date}: {e}")

        start_date = next_date + timedelta(days=1)

    # Combine generation data and map gen_type codes to labels
    df_gen = pd.concat(all_gen).drop_duplicates()
    df_gen["gen_type"] = df_gen["gen_type"].map(GEN_TYPE_LABELS).fillna(df_gen["gen_type"])
    df = df_gen.copy()

    # Combine load and price
    df_load = pd.concat(all_load).drop_duplicates()
    df_price = pd.concat(all_price).drop_duplicates()

    # Merge everything by timestamp
    df = df.merge(df_load, on="timestamp", how="left")
    df = df.merge(df_price, on="timestamp", how="left")

    # Replace NaNs with None for SQL compatibility
    df = df.where(pd.notnull(df), None)

    # Connect to RDS
    db_host = os.environ['DB_HOST']
    db_name = os.environ['DB_NAME']
    db_user = os.environ['DB_USER']
    db_pass = os.environ['DB_PASS']
    db_port = int(os.environ.get('DB_PORT', 5432))  # Ensure it's an int

    # Connect to PostgreSQL
    connection = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_pass,
        port=db_port
    )

    cursor = connection.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS energy_data (
            timestamp TIMESTAMPTZ,
            gen_type TEXT,
            quantity_mw NUMERIC,
            load_mw NUMERIC,
            price_eur_per_mwh NUMERIC,
            PRIMARY KEY (timestamp, gen_type)
        );
    """)
    connection.commit()

    inserted = 0

    for _, row in df.iterrows():
        try:
            print(f"Inserting: {row['timestamp']} | {row['gen_type']} | {row['quantity_mw']} MW")

            cursor.execute(
                """
                INSERT INTO energy_data (
                    timestamp, gen_type, quantity_mw, load_mw, price_eur_per_mwh
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, gen_type)
                DO UPDATE SET
                    quantity_mw = EXCLUDED.quantity_mw,
                    load_mw = EXCLUDED.load_mw,
                    price_eur_per_mwh = EXCLUDED.price_eur_per_mwh;
                """,
                (
                    row['timestamp'],
                    row['gen_type'],
                    row['quantity_mw'],
                    row['load_mw'],
                    row['price_eur_per_mwh']
                )
            )
            inserted += 1

        except Exception as e:
            print(f"Insert error for {row['timestamp']} | {row['gen_type']}: {e}")
            connection.rollback()


    connection.commit()
    cursor.close()
    connection.close()

    return {
        "statusCode": 200,
        "body": f"{inserted} rows inserted to energy_data"
    }
