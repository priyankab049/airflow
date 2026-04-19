from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime
import requests

# Task 1: Fetch Bitcoin price and push via XCom
def fetch_price(**context):
    url = Variable.get(
        "bitcoin_api_url",
        default_var="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    )
    response = requests.get(url, timeout=10)
    data = response.json()
    price = data["bitcoin"]["usd"]
    print(f"Fetched Bitcoin Price: ${price}")

    # Push to XCom
    context["ti"].xcom_push(key="bitcoin_price", value=price)
    return price

# Task 2: Pull price and decide HIGH or LOW
def process_price(**context):
    # Pull from XCom
    price = context["ti"].xcom_pull(
        task_ids="fetch_price",
        key="bitcoin_price"
    )
    print(f"Processing price: ${price}")

    # Business logic
    if price > 50000:
        decision = "HIGH"
    else:
        decision = "LOW"

    print(f"Decision: Bitcoin is {decision}")

    # Push decision via XCom
    context["ti"].xcom_push(key="price_decision", value=decision)
    return decision

# Task 3: Pull decision and save result
def save_result(**context):
    # Pull both XComs
    price = context["ti"].xcom_pull(
        task_ids="fetch_price",
        key="bitcoin_price"
    )
    decision = context["ti"].xcom_pull(
        task_ids="process_price",
        key="price_decision"
    )

    print(f"Final Result:")
    print(f"Bitcoin Price: ${price}")
    print(f"Market Status: {decision}")
    print(f"Saving to database... Done!")

# DAG definition
with DAG(
    dag_id="xcoms_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_task = PythonOperator(
        task_id="fetch_price",
        python_callable=fetch_price,
    )

    process_task = PythonOperator(
        task_id="process_price",
        python_callable=process_price,
    )

    save_task = PythonOperator(
        task_id="save_result",
        python_callable=save_result,
    )

    end = EmptyOperator(task_id="end")

    # Dependencies
    start >> fetch_task >> process_task >> save_task >> end