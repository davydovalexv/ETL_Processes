from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta

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


def incremental_load(**context):
    logger = logging.getLogger("airflow.task")
    
    days = int(os.environ.get("INCREMENTAL_DAYS", "7"))
    
    base_path = os.path.join(
        os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
        "dags", "data"
    )
    
    source_file = os.path.join(base_path, "transformed", "iot_temp_transformed.csv")
    target_file = os.path.join(base_path, "loaded", "iot_temp_loaded.csv")
    
    try:
        if not os.path.exists(source_file):
            error_msg = f"Файл не найден: {source_file}"
            write_log("errors.log", f"incremental_load: {error_msg}")
            raise FileNotFoundError(error_msg)
        
        df_source = pd.read_csv(source_file)
        df_source["noted_date"] = pd.to_datetime(df_source["noted_date"]).dt.date
        
        cutoff = datetime.now().date() - timedelta(days=days)
        df_new = df_source[df_source["noted_date"] >= cutoff].copy()
        
        if len(df_new) == 0:
            msg = f"incremental_load: Нет данных за последние {days} дней"
            write_log("success.log", msg)
            logger.info(msg)
            return
        
        if os.path.exists(target_file):
            df_existing = pd.read_csv(target_file)
            df_existing["noted_date"] = pd.to_datetime(df_existing["noted_date"]).dt.date
            
            min_date = df_new["noted_date"].min()
            max_date = df_new["noted_date"].max()
            
            df_existing = df_existing[
                ~((df_existing["noted_date"] >= min_date) & (df_existing["noted_date"] <= max_date))
            ]
            
            df_result = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_result = df_new
        
        os.makedirs(os.path.dirname(target_file), exist_ok=True)
        df_result.to_csv(target_file, index=False)
        
        count = len(df_new)
        total = len(df_result)
        success_msg = f"incremental_load: Добавлено {count} строк, всего {total} строк в {target_file}"
        write_log("success.log", success_msg)
        logger.info(success_msg)
        
    except Exception as e:
        error_msg = f"incremental_load: Ошибка - {str(e)}"
        write_log("errors.log", error_msg)
        logger.error(error_msg)
        raise


with DAG(
    dag_id="etl_iot_temp_load_incremental",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["homework", "etl", "load", "incremental"],
    description="Инкрементальная загрузка преобразованных данных в файлы",
) as dag:

    load_task = PythonOperator(
        task_id="incremental_load",
        python_callable=incremental_load,
    )

    load_task
