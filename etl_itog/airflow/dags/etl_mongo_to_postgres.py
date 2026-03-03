from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import psycopg2

MONGO_CONN = "mongodb://mongo:27017/"
POSTGRES_CONN = {
    "host": "postgres",
    "port": 5432,
    "database": "etl_db",
    "user": "etl_user",
    "password": "etl_pass"
}

def load_mongo_to_postgres(**context):
    mongo = MongoClient(MONGO_CONN)
    db = mongo['etl_demo']

    pg = psycopg2.connect(**POSTGRES_CONN)
    pg.autocommit = True
    cur = pg.cursor()
    # Load UserSessions
    for sess in db.UserSessions.find():
        cur.execute("""
            INSERT INTO user_sessions 
            (session_id, user_id, start_time, end_time, pages_visited, device, actions) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id) DO NOTHING;
        """, (
            sess["session_id"],
            sess["user_id"],
            sess["start_time"],
            sess["end_time"],
            sess.get("pages_visited", []),
            str(sess.get("device", {})),
            sess.get("actions", [])
        ))
    # Repeat for EventLogs
    for evt in db.EventLogs.find():
        cur.execute("""
            INSERT INTO event_logs (event_id, timestamp, event_type, details)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING;
        """, (
            evt["event_id"], evt["timestamp"], evt["event_type"], str(evt.get("details", {}))
        ))
    # SupportTickets
    for ticket in db.SupportTickets.find():
        cur.execute("""
            INSERT INTO support_tickets (ticket_id, user_id, status, issue_type, messages, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO NOTHING;
        """, (
            ticket["ticket_id"], ticket["user_id"], ticket["status"], ticket["issue_type"],
            str(ticket["messages"]), ticket["created_at"], ticket["updated_at"]
        ))
    # Recommendations
    for rec in db.UserRecommendations.find():
        cur.execute("""
            INSERT INTO user_recommendations (user_id, recommended_products, last_updated)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (
            rec["user_id"], rec["recommended_products"], rec["last_updated"]
        ))
    # ModerationQueue
    for review in db.ModerationQueue.find():
        cur.execute("""
            INSERT INTO moderation_queue (review_id, user_id, product_id, review_text, rating, moderation_status, flags, submitted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (review_id) DO NOTHING;
        """, (
            review["review_id"], review["user_id"], review["product_id"], review["review_text"], review["rating"], review["moderation_status"], review["flags"], review["submitted_at"]
        ))
    cur.close()
    pg.close()

default_args = {
    'owner': 'etl',
    'start_date': datetime(2024, 1, 10),
}

with DAG('etl_mongo_to_postgres', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    etl_task = PythonOperator(
        task_id='replicate_mongo_postgres',
        python_callable=load_mongo_to_postgres,
    )