from __future__ import annotations

import os
import logging
from datetime import datetime

import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def transform_iot_temp(**context) -> None:
    """
    Шаги:
    1) читаем CSV
    2) фильтр out/in == in (без учета регистра)
    3) noted_date -> date (yyyy-MM-dd)
    4) чистим temp по 5 и 95 процентилю
    5) считаем 5 самых жарких и 5 самых холодных дней по средней температуре дня
    """

    logger = logging.getLogger("airflow.task")

    # 1) Где лежит CSV
    # По умолчанию: <AIRFLOW_HOME>/dags/data/IOT-temp.csv (часто /opt/airflow/dags/data/IOT-temp.csv)
    default_path = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dags", "data", "IOT-temp.csv")
    csv_path = os.environ.get("IOT_TEMP_CSV", default_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"CSV не найден: {csv_path}. "
            f"Положите файл в dags/data/IOT-temp.csv или задайте env IOT_TEMP_CSV."
        )

    logger.info("Читаю CSV: %s", csv_path)
    df = pd.read_csv(csv_path)

    # Базовая проверка колонок
    required_cols = {"noted_date", "temp", "out/in"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"В CSV нет обязательных колонок {missing}. Колонки в файле: {list(df.columns)}")

    logger.info("Строк до фильтраций: %d", len(df))

    # 2) Фильтр out/in = in (в данных: In/Out)
    df["out/in_norm"] = df["out/in"].astype(str).str.strip().str.lower()
    df = df[df["out/in_norm"] == "in"].copy()
    logger.info("После фильтра out/in == in: %d", len(df))

    # 3) noted_date -> date yyyy-MM-dd
    # В файле формат похож на dd-mm-yyyy HH:MM, поэтому dayfirst=True
    dt_series = pd.to_datetime(df["noted_date"], dayfirst=True, errors="coerce")
    bad_dt = dt_series.isna().sum()
    if bad_dt:
        logger.warning("Не удалось распарсить noted_date у %d строк — они будут удалены", bad_dt)
    df["noted_date_dt"] = dt_series
    df = df.dropna(subset=["noted_date_dt"]).copy()

    # Берем только дату (тип date). Для вывода yyyy-MM-dd Airflow/лог сам отобразит нормально,
    # а при необходимости можно df["noted_date"].astype(str) получить в yyyy-mm-dd.
    df["noted_date"] = df["noted_date_dt"].dt.date

    # 4) Чистка temp по 5 и 95 процентилю
    # temp в файле выглядит числом, но на всякий случай приведем к numeric
    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    before_temp_na = len(df)
    df = df.dropna(subset=["temp"]).copy()
    if len(df) != before_temp_na:
        logger.info("Удалено строк с некорректной temp: %d", before_temp_na - len(df))

    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    logger.info("Percentiles temp: p5=%.3f, p95=%.3f", p5, p95)

    before_clip = len(df)
    df = df[(df["temp"] >= p5) & (df["temp"] <= p95)].copy()
    logger.info("После чистки по [p5, p95]: %d (удалено %d)", len(df), before_clip - len(df))

    # 5) 5 самых жарких / холодных дней
    # Считаем среднюю температуру дня
    daily = (
        df.groupby("noted_date", as_index=False)
          .agg(
              avg_temp=("temp", "mean"),
              min_temp=("temp", "min"),
              max_temp=("temp", "max"),
              measurements=("temp", "count"),
          )
    )

    # На случай если данных меньше 5 дней
    top_n = min(5, len(daily))

    hottest = daily.sort_values("avg_temp", ascending=False).head(top_n)
    coldest = daily.sort_values("avg_temp", ascending=True).head(top_n)

    logger.info("=== 5 самых жарких дней (по средней температуре дня) ===")
    for row in hottest.to_dict("records"):
        logger.info(
            "date=%s avg=%.3f min=%.3f max=%.3f n=%d",
            row["noted_date"], row["avg_temp"], row["min_temp"], row["max_temp"], row["measurements"]
        )

    logger.info("=== 5 самых холодных дней (по средней температуре дня) ===")
    for row in coldest.to_dict("records"):
        logger.info(
            "date=%s avg=%.3f min=%.3f max=%.3f n=%d",
            row["noted_date"], row["avg_temp"], row["min_temp"], row["max_temp"], row["measurements"]
        )

    # Доп. информация (полезно для самопроверки)
    logger.info("Уникальных дней в датасете после обработки: %d", daily["noted_date"].nunique())
    logger.info("Диапазон дат: %s .. %s", daily["noted_date"].min(), daily["noted_date"].max())

    # Сохраняем преобразованные данные для последующей загрузки
    # Удаляем служебные колонки перед сохранением и переименовываем колонки для удобства
    df_final = df[["id", "room_id/id", "noted_date", "temp"]].copy()
    df_final = df_final.rename(columns={"room_id/id": "room_id"})
    
    # Путь для сохранения преобразованных данных
    output_dir = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dags", "data", "transformed")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "iot_temp_transformed.csv")
    
    # Сохраняем в CSV (noted_date будет сохранен как строка в формате yyyy-MM-dd)
    df_final["noted_date"] = df_final["noted_date"].astype(str)
    df_final.to_csv(output_path, index=False)
    logger.info("Преобразованные данные сохранены в: %s (строк: %d)", output_path, len(df_final))


# AIRFLOW
with DAG(
    dag_id="etl_iot_temp_transform",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # запуск вручную (напр. каждый день - @daily)
    catchup=False,  # позволяет запускать DAG в прошлом
    tags=["homework", "etl", "transform"],
    description="HW: фильтрация In, парсинг даты, чистка temp по p5/p95, top5 hot/cold days",
) as dag:

    run_transform = PythonOperator(
        task_id="transform_iot_temp",
        python_callable=transform_iot_temp,
    )

    run_transform