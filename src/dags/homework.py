from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

import random
import psycopg2
from pymongo.mongo_client import MongoClient

from models.heat_and_humidity import HeatAndHumidityMeasureEvent

# Connection constants
MONGO_CONN_STR = "mongodb+srv://cetingokhan:cetingokhan@cluster0.ff5aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"


def generate_random_heat_and_humidity_data(dummy_record_count: int):
    records = []
    for _ in range(dummy_record_count):
        temperature = random.randint(10, 40)
        humidity = random.randint(10, 100)
        timestamp = datetime.now()
        creator = "nidaduman"
        record = HeatAndHumidityMeasureEvent(temperature, humidity, timestamp, creator)
        records.append(record)
    return records


def save_data_to_mongodb(records):
    client = MongoClient(MONGO_CONN_STR)
    db = client["bigdata_training"]
    collection = db["user_coll_nidaduman"]
    for record in records:
        collection.insert_one(record._dict_)
    client.close()


def create_sample_data_on_mongodb():
    records = generate_random_heat_and_humidity_data(10)
    save_data_to_mongodb(records)


def copy_anomalies_into_new_collection():
    client = MongoClient(MONGO_CONN_STR)
    db = client["bigdata_training"]
    source = db["user_coll_nidaduman"]
    destination = db["anomalies_nidaduman"]

    anomalies = source.find({"temperature": {"$gt": 30}})
    for record in anomalies:
        record.pop("_id", None)
        record["creator"] = "nidaduman"
        destination.insert_one(record)

    client.close()


def copy_airflow_logs_into_new_collection():
    
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cur = conn.cursor()

    now = datetime.now()
    one_minute_ago = now - timedelta(minutes=1)

    query = """
        SELECT event, COUNT(*)
        FROM log
        WHERE execution_date >= %s
        GROUP BY event
    """
    cur.execute(query, (one_minute_ago,))
    rows = cur.fetchall()

   
    client = MongoClient(MONGO_CONN_STR)
    db = client["bigdata_training"]
    collection = db["log_nidaduman"]

    for event_name, count in rows:
        doc = {
            "event_name": event_name,
            "record_count": count,
            "created_at": now
        }
        collection.insert_one(doc)

   
    cur.close()
    conn.close()
    client.close()


with DAG(
    dag_id="homework",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    schedule_interval="*/5 * * * *",
) as dag:

    dag_start = DummyOperator(task_id="start")

    create_data = PythonOperator(
        task_id="create_sample_data",
        python_callable=create_sample_data_on_mongodb,
    )

    copy_anomalies = PythonOperator(
        task_id="copy_anomalies",
        python_callable=copy_anomalies_into_new_collection,
    )

    copy_logs = PythonOperator(
        task_id="insert_airflow_logs",
        python_callable=copy_airflow_logs_into_new_collection,
    )

    dag_end = DummyOperator(task_id="finaltask")

    dag_start >> create_data >> copy_anomalies >> dag_end
    dag_start >> copy_logs >> dag_end