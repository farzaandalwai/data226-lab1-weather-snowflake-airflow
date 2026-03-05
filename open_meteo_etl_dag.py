from __future__ import annotations
import os
import json
from datetime import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

OPEN_METEO_URL="https://api.open-meteo.com/v1/forecast" 

def _ensure_column(cur, table_fqn: str, col_name: str, col_type: str) -> None:
    cur.execute(f"DESC TABLE {table_fqn};")
    cols = {row[0].upper() for row in cur.fetchall()}
    if col_name.upper() not in cols:
        cur.execute(f"ALTER TABLE {table_fqn} ADD COLUMN {col_name} {col_type};")


def etl_open_meteo_to_weather_data():
    raw_locations= Variable.get("OM_LOCATIONS") 
    locations =json.loads(raw_locations) 
    if not isinstance(locations, list) or len(locations) < 2:
        raise ValueError("OM_LOCATIONS must be a JSON LIST with at least 2 locations.")

    stage_name=Variable.get("SF_STAGE") 

    hook =SnowflakeHook(snowflake_conn_id="snowflake_lab1")
    conn =hook.get_conn()
    cur= conn.cursor() 

    try:
        cur.execute("USE ROLE TRAINING_ROLE;") 
        cur.execute("USE WAREHOUSE BLUEJAY;") 
        cur.execute("USE DATABASE USER_DB_BLUEJAY;")
        cur.execute("USE SCHEMA RAW;")

        for col,typ in [
            ("LOCATION_NAME", "STRING"),
            ("CITY", "STRING"), 
            ("LATITUDE","FLOAT"),
            ("LONGITUDE","FLOAT"),
            ("TEMP_MEAN","FLOAT"),
        ]:
            _ensure_column(cur, "RAW.WEATHER_DATA", col, typ) 
            _ensure_column(cur, "RAW.WEATHER_DAILY", col, typ) 

        for loc in locations:
            location_name =loc["location_name"]
            city= location_name 
            lat =float(loc["latitude"])
            lon=float(loc["longitude"])

            params = {
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,weather_code",
                "timezone": "auto",
                "past_days": 90,
                "forecast_days": 0,
            }
            r = requests.get(OPEN_METEO_URL, params=params, timeout=30)
            r.raise_for_status()
            daily = r.json()["daily"]

            df = pd.DataFrame({
                "LOCATION_NAME": location_name,
                "CITY": city,
                "LATITUDE": lat,
                "LONGITUDE": lon,
                "DATE": pd.to_datetime(pd.Series(daily["time"])).dt.date,
                "TEMP_MAX": pd.to_numeric(daily["temperature_2m_max"], errors="coerce"),
                "TEMP_MIN": pd.to_numeric(daily["temperature_2m_min"], errors="coerce"),
                "TEMP_MEAN": pd.to_numeric(daily["temperature_2m_mean"], errors="coerce"),
                "PRECIPITATION": pd.to_numeric(daily["precipitation_sum"], errors="coerce"),
                "WEATHER_CODE": pd.to_numeric(daily["weather_code"], errors="coerce"),
            })

            df = df[
                [
                    "LOCATION_NAME",
                    "CITY",
                    "LATITUDE",
                    "LONGITUDE",
                    "DATE",
                    "TEMP_MAX",
                    "TEMP_MIN",
                    "TEMP_MEAN",
                    "PRECIPITATION",
                    "WEATHER_CODE",
                ]
            ]

            safe_name =location_name.replace(" ", "_").replace(",", "")
            tmp_path= f"/tmp/open_meteo_{safe_name}.csv"
            df.to_csv(tmp_path,index=False)
            staged_file= os.path.basename(tmp_path) + ".gz" 
            staged_pattern= ".*" + staged_file.replace(".", "\\.") + "$"

            try:
                cur.execute("BEGIN;")
                cur.execute("TRUNCATE TABLE RAW.WEATHER_DAILY;")

                cur.execute(f"PUT file://{tmp_path} @{stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;")
                cur.execute(f"""
                    COPY INTO RAW.WEATHER_DAILY
                    (LOCATION_NAME, CITY, LATITUDE, LONGITUDE, DATE, TEMP_MAX, TEMP_MIN, TEMP_MEAN, PRECIPITATION, WEATHER_CODE)
                    FROM @{stage_name}
                    PATTERN = '{staged_pattern}'
                    FILE_FORMAT = (
                        TYPE = CSV
                        FIELD_DELIMITER = ','
                        SKIP_HEADER = 1
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                        NULL_IF = ('', 'null', 'NULL')
                        EMPTY_FIELD_AS_NULL = TRUE
                        TRIM_SPACE = TRUE
                    )
                    ON_ERROR = 'ABORT_STATEMENT'
                    PURGE = TRUE;
                """)
                cur.execute("""
                    DELETE FROM RAW.WEATHER_DATA
                    WHERE LOCATION_NAME = %s
                      AND DATE >= DATEADD('day', -90, CURRENT_DATE());
                """, (location_name,)) 

                cur.execute("""
                    MERGE INTO RAW.WEATHER_DATA t
                    USING RAW.WEATHER_DAILY s
                      ON t.LOCATION_NAME = s.LOCATION_NAME AND t.DATE = s.DATE
                    WHEN MATCHED THEN UPDATE SET
                      t.CITY = s.CITY,
                      t.LATITUDE = s.LATITUDE,
                      t.LONGITUDE =s.LONGITUDE,
                      t.TEMP_MAX =s.TEMP_MAX,
                      t.TEMP_MIN= s.TEMP_MIN,
                      t.TEMP_MEAN= s.TEMP_MEAN,
                      t.PRECIPITATION = s.PRECIPITATION,
                      t.WEATHER_CODE= s.WEATHER_CODE
                    WHEN NOT MATCHED THEN INSERT
                      (LOCATION_NAME, CITY, LATITUDE, LONGITUDE, DATE, TEMP_MAX, TEMP_MIN, TEMP_MEAN, PRECIPITATION, WEATHER_CODE)
                    VALUES
                      (s.LOCATION_NAME, s.CITY, s.LATITUDE, s.LONGITUDE, s.DATE, s.TEMP_MAX, s.TEMP_MIN, s.TEMP_MEAN, s.PRECIPITATION, s.WEATHER_CODE);
                """)

                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

    finally:
        try:
            cur.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass


with DAG(
    dag_id="open_meteo_etl_dag",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    tags=["lab1","etl"],
) as dag:
    PythonOperator(
        task_id="etl_open_meteo_to_weather_data",
        python_callable=etl_open_meteo_to_weather_data,
    )