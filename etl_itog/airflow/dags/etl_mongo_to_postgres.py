import os
import json
import psycopg2
from psycopg2.extras import Json
from pymongo import MongoClient
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "etl_pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "etl_db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "etl_demo")

default_args = {
    'owner': 'etl',
    'retries': 0,
}

def _serialize_for_json(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def load_mongo_to_postgres(**kwargs):
    # Подключение к MongoDB
    mongo_client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    mongo_db = mongo_client[MONGO_DB]

    # Подключение к PostgreSQL
    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )
    pg_conn.autocommit = True
    cur = pg_conn.cursor()

    # Загрузка коллекции UserSessions в таблицу user_sessions
    for session in mongo_db.UserSessions.find():
        cur.execute(
            """
            INSERT INTO user_sessions (
                session_id,
                user_id,
                start_time,
                end_time,
                pages_visited,
                device,
                actions
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING
            """,
            (
                session.get("session_id"),
                session.get("user_id"),
                session.get("start_time"),
                session.get("end_time"),
                session.get("pages_visited", []),
                Json(session.get("device", {}), dumps=lambda o: json.dumps(o, default=_serialize_for_json)),
                session.get("actions", []),
            ),
        )

    # Загрузка коллекции SupportTickets в таблицу support_tickets
    for ticket in mongo_db.SupportTickets.find():
        # Преобразуем сообщения, чтобы внутри не было "сырых" datetime
        raw_messages = ticket.get("messages", [])
        safe_messages = json.loads(
            json.dumps(raw_messages, default=_serialize_for_json)
        )

        cur.execute(
            """
            INSERT INTO support_tickets (
                ticket_id,
                user_id,
                status,
                issue_type,
                messages,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO NOTHING
            """,
            (
                ticket.get("ticket_id"),
                ticket.get("user_id"),
                ticket.get("status"),
                ticket.get("issue_type"),
                Json(safe_messages),
                ticket.get("created_at"),
                ticket.get("updated_at"),
            ),
        )

    cur.close()
    pg_conn.close()
    mongo_client.close()

with DAG(
    dag_id='etl_mongo_to_postgres',
    default_args=default_args,
    description='ETL: MongoDB → Postgres',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl'],
) as dag:
    replicate_mongo_postgres = PythonOperator(
        task_id='replicate_mongo_postgres',
        python_callable=load_mongo_to_postgres,
        provide_context=True
    )