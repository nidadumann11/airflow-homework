from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def selam_ver():
    print("🛫 Airflow çalişti! Merhaba!")

# DAG tanımı
with DAG(
    dag_id="hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["ornek", "baslangic"]
) as dag:

    gorev = PythonOperator(
        task_id="selamlar",
        python_callable=selam_ver
    )