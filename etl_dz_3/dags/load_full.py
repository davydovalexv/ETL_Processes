from __future__ import annotations

import os
import logging
from datetime import datetime

import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def write_log(log_file, message):
    log_path = os.path.join(
        os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
        "dags", "data", "logs", log_file
    )
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()} - {message}\n")


def full_load(**context):
    logger = logging.getLogger("airflow.task")
    
    base_path = os.path.join(
        os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
        "dags", "data"
    )
    
    source_file = os.path.join(base_path, "transformed", "iot_temp_transformed.csv")
    target_file = os.path.join(base_path, "loaded", "iot_temp_loaded.csv")
    
    try:
        if not os.path.exists(source_file):
            error_msg = f"Файл не найден: {source_file}"
            write_log("errors.log", f"full_load: {error_msg}")
            raise FileNotFoundError(error_msg)
        
        df = pd.read_csv(source_file)
        df["noted_date"] = pd.to_datetime(df["noted_date"]).dt.date
        
        os.makedirs(os.path.dirname(target_file), exist_ok=True)
        df.to_csv(target_file, index=False)
        
        count = len(df)
        success_msg = f"full_load: Загружено {count} строк в {target_file}"
        write_log("success.log", success_msg)
        logger.info(success_msg)
        
    except Exception as e:
        error_msg = f"full_load: Ошибка - {str(e)}"
        write_log("errors.log", error_msg)
        logger.error(error_msg)
        raise


with DAG(
    dag_id="etl_iot_temp_load_full",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["homework", "etl", "load", "full"],
    description="Полная загрузка преобразованных данных в файлы",
) as dag:

    load_task = PythonOperator(
        task_id="full_load",
        python_callable=full_load,
    )

    load_task
