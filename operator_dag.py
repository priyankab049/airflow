from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests

# Task 1: Fetch Bitcoin price
def get_bitcoin_price():
    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    response = requests.get(url)
    data = response.json()
    price = data["bpi"]["USD"]["rate"]
    print(f"Current Bitcoin Price: ${price}")
    return price

# DAG definition
with DAG(
    dag_id="operators_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # EmptyOperator as start marker
    start = EmptyOperator(
        task_id="start"
    )

    # PythonOperator - fetch bitcoin price
    fetch_price = PythonOperator(
        task_id="fetch_bitcoin_price",
        python_callable=get_bitcoin_price,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    # BashOperator - print timestamp
    print_timestamp = BashOperator(
        task_id="print_timestamp",
        bash_command="echo Current timestamp is $(date)",
    )

    # EmptyOperator as end marker
    end = EmptyOperator(
        task_id="end"
    )

    # Task dependencies
    start >> fetch_price >> print_timestamp >> end