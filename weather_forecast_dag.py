from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def run_forecast_pipeline():
    horizon =int(Variable.get("FORECAST_HORIZON_DAYS", default_var="7"))
    target_col= Variable.get("TARGET_COL", default_var="TEMP_MEAN").upper()
    if horizon <= 0:
        raise ValueError(f"FORECAST_HORIZON_DAYS must be > 0, got {horizon}")

    allowed_targets= {"TEMP_MAX", "TEMP_MIN", "TEMP_MEAN", "PRECIPITATION"}
    if target_col not in allowed_targets:
        raise ValueError(
            f"TARGET_COL must be one of {sorted(allowed_targets)}; got {target_col}"
        )

    hook =SnowflakeHook(snowflake_conn_id="snowflake_lab1")
    conn= hook.get_conn()
    cur =conn.cursor()

    try:
        cur.execute("USE ROLE TRAINING_ROLE;")
        cur.execute("USE WAREHOUSE BLUEJAY;")
        cur.execute("USE DATABASE USER_DB_BLUEJAY;")
        cur.execute("USE SCHEMA RAW;")

        cur.execute(
            f"""
            CREATE OR REPLACE VIEW RAW.V_WEATHER_TRAIN AS
            SELECT
              TO_VARIANT(LOCATION_NAME) AS SERIES,
              TO_TIMESTAMP_NTZ(DATE) AS TS,
              {target_col}::FLOAT AS Y
            FROM RAW.WEATHER_DATA
            WHERE {target_col} IS NOT NULL;
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS RAW.WEATHER_FORECAST (
              SERIES VARIANT,
              TS TIMESTAMP_NTZ,
              FORECAST FLOAT,
              LOWER_BOUND FLOAT,
              UPPER_BOUND FLOAT
            );
            """
        )

        cur.execute(
            """
            CREATE OR REPLACE SNOWFLAKE.ML.FORECAST RAW.WEATHER_TEMP_MODEL(
              INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'RAW.V_WEATHER_TRAIN'),
              SERIES_COLNAME => 'SERIES',
              TIMESTAMP_COLNAME => 'TS',
              TARGET_COLNAME => 'Y'
            );
            """
        )

        cur.execute("BEGIN;")
        cur.execute("DELETE FROM RAW.WEATHER_FORECAST;")
        cur.execute(
            f"""
            INSERT INTO RAW.WEATHER_FORECAST (SERIES, TS, FORECAST, LOWER_BOUND, UPPER_BOUND)
            SELECT SERIES, TS, FORECAST, LOWER_BOUND, UPPER_BOUND
            FROM TABLE(
              RAW.WEATHER_TEMP_MODEL!FORECAST(FORECASTING_PERIODS => {horizon})
            );
            """
        )
        cur.execute("COMMIT;")

    except Exception:
        try:
            cur.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def build_weather_final():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_lab1")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN;")
        cur.execute("USE ROLE TRAINING_ROLE;")
        cur.execute("USE WAREHOUSE BLUEJAY;")
        cur.execute("USE DATABASE USER_DB_BLUEJAY;")
        cur.execute("USE SCHEMA RAW;")
        cur.execute(
            """
            CREATE OR REPLACE TABLE RAW.WEATHER_FINAL AS
            SELECT
            LOCATION_NAME,
            DATE AS DS,
            TEMP_MAX::FLOAT AS ACTUAL,
            NULL::FLOAT AS FORECAST,
            NULL::FLOAT AS LOWER_BOUND,
            NULL::FLOAT AS UPPER_BOUND
            FROM RAW.WEATHER_DATA
            UNION ALL
            SELECT
            SERIES::STRING AS LOCATION_NAME,
            TO_DATE(TS) AS DS,
            NULL::FLOAT AS ACTUAL,
            FORECAST::FLOAT AS FORECAST,
            LOWER_BOUND::FLOAT AS LOWER_BOUND,
            UPPER_BOUND::FLOAT AS UPPER_BOUND
            FROM RAW.WEATHER_FORECAST;
            """
        )
        cur.execute("COMMIT;")
    except Exception:
        try:
            cur.execute("ROLLBACK;")
        except Exception:
            pass
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


with DAG(
    dag_id="weather_forecast_dag",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    tags=["lab1", "forecast"],
) as dag:
    run_forecast_pipeline = PythonOperator(
        task_id="run_forecast_pipeline",
        python_callable=run_forecast_pipeline,
    )
    build_weather_final = PythonOperator(
        task_id="build_weather_final",
        python_callable=build_weather_final,
    )

    run_forecast_pipeline >> build_weather_final
