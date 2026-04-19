from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Task 1: Create table in Postgres
def create_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("""
        CREATE TABLE IF NOT EXISTS bitcoin_prices (
            id SERIAL PRIMARY KEY,
            price FLOAT,
            market_status VARCHAR(10),
            recorded_at TIMESTAMP DEFAULT NOW()
        );
    """)
    print("Table created successfully!")

# Task 2: Fetch Bitcoin price using HttpHook
def fetch_price(**context):
    hook = HttpHook(http_conn_id="coingecko_api")
    response = hook.run(
        "/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    )
    data = response.json()
    price = data["bitcoin"]["usd"]
    print(f"Fetched Bitcoin Price: ${price}")

    # Push to XCom
    context["ti"].xcom_push(key="bitcoin_price", value=price)
    return price

# Task 3: Save price to Postgres
def save_to_db(**context):
    # Pull price from XCom
    price = context["ti"].xcom_pull(
        task_ids="fetch_price",
        key="bitcoin_price"
    )

    # Determine market status
    status = "HIGH" if price > 50000 else "LOW"

    # Save to Postgres
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run(
        """
        INSERT INTO bitcoin_prices (price, market_status)
        VALUES (%s, %s);
        """,
        parameters=[price, status]
    )
    print(f"Saved price ${price} with status {status} to database!")

# Task 4: Verify data was saved
def verify_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records(
        "SELECT id, price, market_status, recorded_at FROM bitcoin_prices ORDER BY recorded_at DESC LIMIT 5;"
    )
    print("Latest 5 records in database:")
    for record in records:
        print(f"  ID: {record[0]} | Price: ${record[1]} | Status: {record[2]} | Time: {record[3]}")

# DAG definition
with DAG(
    dag_id="connections_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    fetch_price_task = PythonOperator(
        task_id="fetch_price",
        python_callable=fetch_price,
    )

    save_to_db_task = PythonOperator(
        task_id="save_to_db",
        python_callable=save_to_db,
    )

    verify_data_task = PythonOperator(
        task_id="verify_data",
        python_callable=verify_data,
    )

    end = EmptyOperator(task_id="end")

    # Dependencies
    start >> create_table_task >> fetch_price_task >> save_to_db_task >> verify_data_task >> end