from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Task functions
def extract():
    print("Extracting data from source...")
    data = {"records": 100, "source": "bitcoin_api"}
    print(f"Extracted: {data}")
    return data
    
def transform_a():
    print("Transform A: Cleaning data...")
    print("Removed nulls, fixed data types")

def transform_b():
    print("Transform B: Aggregating data...")
    print("Calculated averages and totals")

def load():
    print("Loading data into warehouse...")
    print("100 records inserted successfully")

def notify():
    print("Pipeline completed! Sending notification...")
    print("Email sent to team@company.com")

# DAG definition
with DAG(
    dag_id="dependencies_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # Tasks
    start = EmptyOperator(
        task_id="start"
    )

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_a_task = PythonOperator(
        task_id="transform_a",
        python_callable=transform_a,
    )

    transform_b_task = PythonOperator(
        task_id="transform_b",
        python_callable=transform_b,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
        retries=3,
        retry_delay=timedelta(minutes=1),
    )

    notify_task = PythonOperator(
        task_id="notify",
        python_callable=notify,
        trigger_rule="all_done",  # runs even if something fails
    )

    end = EmptyOperator(
        task_id="end"
    )

    # Dependencies - fan-out + fan-in
    start >> extract_task >> [transform_a_task, transform_b_task] >> load_task >> notify_task >> end