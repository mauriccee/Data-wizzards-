
Data-wizzards
==================

This project provides a suite of serverless tools for fetching, processing, forecasting, and exporting energy and weather data. The workflows are powered by Python and AWS Lambda, following a modular and maintainable architecture.

GitHub Repository Structure
---------------------------
Data-wizzards-/
│
├── energy_data/
│   ├── general_fetch/                      - Scripts and helpers for fetching energy datasets
│   └── daily_fetch/                        - Daily ingestion scripts
│
├── lambda_functions/
│   └── lambda_exports/                     - Shared deployment utilities and all Lambda projects
│       ├── group1_backfill_weather/            - Lambda for historical weather data ingestion
│       ├── group1_daily_energy_forecasting/    - Lambda for daily forecasting with trained models
│       ├── group1_meteo/                       - Lambda for meteorological aggregation and upserts
│       ├── group1_train_model_stores3/         - Lambda for model training and storing results in S3
│       ├── group1_weather_forecast/            - Lambda for daily weather forecast ingestion
│       ├── group1_energy_1/                    - Lambda for ingestion and transformation of energy data
│       ├── group1_energy_2/                    - Lambda for API-based energy data parsing (XML, etc.)
│       └── group1_energy_3/                    - Lambda for energy demand forecasting (statistical modeling)

Per-Lambda Requirements
-----------------------

All Lambdas are compatible with Python 3.11. Below are the dependencies for each function:

1. group1_backfill_weather
   - boto3
   - pandas
   - numpy
   - psycopg2-binary
   - openmeteo-requests
   - requests-cache
   - retry-requests

2. group1_weather_forecast
   - pandas
   - numpy
   - psycopg2-binary
   - openmeteo-requests
   - requests-cache
   - retry-requests

3. group1_meteo
   - pandas
   - numpy
   - (local modules: db, config, fetcher_range, upsert)

4. group1_train_model_stores3
   - boto3
   - pandas
   - holidays
   - statsmodels

5. group1_daily_energy_forecasting
   - boto3
   - pandas
   - psycopg2-binary
   - holidays
   - pickle5

6. group1_energy_1
   - boto3
   - pandas
   - psycopg2-binary
   - holidays
   - pickle5

7. group1_energy_2
   - requests
   - pandas
   - isodate
   - psycopg2-binary

8. group1_energy_3
   - json (built-in)
   - pandas
   - numpy
   - statsmodels

Getting Started
---------------
Prerequisites:
- AWS account and credentials configured
- Python 3.11 (recommended for Lambda compatibility)

Install dependencies on a clean virtual environment (cloud 9)

Packaging Lambda Layers:

    pip install -r [requirements] -t python/
    zip -r layer.zip python/



Usage
-----
Add lambda layers per lambda function

Add code to different lambda functions

Configure Eventsbridge triggers according to frequency need of reporting


License
-------
Free to use 

Contact & Authors
--------------------

Laura Furrer  
Postal Code: 8038  
Email: laura.furrer.01@stud.hslu.com

Sarp Koc  
Postal Code: 5000  
Email: sarp.koc@stud.hslu.ch

Moritz Pfenninger  
Postal Code: 8003  
Email: moritz.pfenninger@stud.hslu.ch

Examiners
---------
- PD Dr. Luis Terán  
- José Mancera  
- Jhonny Vladmir Pincay Nieves

Place and Date
--------------
Lucerne, 08.06.2025

