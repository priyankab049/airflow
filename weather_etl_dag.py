from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests

# Task 1: Create table
def create_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            city VARCHAR(100),
            temperature FLOAT,
            feels_like FLOAT,
            humidity INTEGER,
            weather_description VARCHAR(200),
            wind_speed FLOAT,
            recorded_at TIMESTAMP DEFAULT NOW()
        );
    """)
    print("Weather table created successfully!")

# Task 2: Extract weather data
def extract_weather(**context):
    api_key = Variable.get("weather_api_key")
    city = Variable.get("weather_city")
    base_url = Variable.get("weather_api_base_url")

    url = f"{base_url}/data/2.5/weather"
    params = {
        "q": city,
        "appid": api_key,
        "units": "metric"  # Celsius
    }

    response = requests.get(url, params=params, timeout=10)

    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

    data = response.json()
    print(f"Raw weather data fetched for {city}")
    print(f"Response: {data}")

    # Push raw data via XCom
    context["ti"].xcom_push(key="raw_weather", value=data)
    return data

# Task 3: Transform weather data
def transform_weather(**context):
    # Pull raw data from XCom
    raw_data = context["ti"].xcom_pull(
        task_ids="extract_weather",
        key="raw_weather"
    )

    # Transform and clean
    transformed = {
        "city": raw_data["name"],
        "temperature": raw_data["main"]["temp"],
        "feels_like": raw_data["main"]["feels_like"],
        "humidity": raw_data["main"]["humidity"],
        "weather_description": raw_data["weather"][0]["description"],
        "wind_speed": raw_data["wind"]["speed"]
    }

    print(f"Transformed weather data:")
    print(f"  City: {transformed['city']}")
    print(f"  Temperature: {transformed['temperature']}°C")
    print(f"  Feels Like: {transformed['feels_like']}°C")
    print(f"  Humidity: {transformed['humidity']}%")
    print(f"  Description: {transformed['weather_description']}")
    print(f"  Wind Speed: {transformed['wind_speed']} m/s")

    # Push transformed data via XCom
    context["ti"].xcom_push(key="transformed_weather", value=transformed)
    return transformed

# Task 4: Load into Postgres
def load_weather(**context):
    # Pull transformed data from XCom
    data = context["ti"].xcom_pull(
        task_ids="transform_weather",
        key="transformed_weather"
    )

    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run(
        """
        INSERT INTO weather_data 
            (city, temperature, feels_like, humidity, weather_description, wind_speed)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        parameters=[
            data["city"],
            data["temperature"],
            data["feels_like"],
            data["humidity"],
            data["weather_description"],
            data["wind_speed"]
        ]
    )
    print(f"Weather data loaded successfully!")

# Task 5: Verify loaded data
def verify_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records(
        """
        SELECT city, temperature, feels_like, humidity, 
               weather_description, wind_speed, recorded_at 
        FROM weather_data 
        ORDER BY recorded_at DESC 
        LIMIT 5;
        """
    )
    print("Latest 5 weather records:")
    print("-" * 60)
    for record in records:
        print(f"City: {record[0]}")
        print(f"Temperature: {record[1]}°C | Feels Like: {record[2]}°C")
        print(f"Humidity: {record[3]}% | Wind: {record[5]} m/s")
        print(f"Condition: {record[4]}")
        print(f"Recorded: {record[6]}")
        print("-" * 60)

# DAG definition
with DAG(
    dag_id="weather_etl_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "owner": "priyanka"
    },
    tags=["etl", "weather", "portfolio"]
) as dag:

    start = EmptyOperator(task_id="start")

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table,
    )

    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    load_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    verify_task = PythonOperator(
        task_id="verify_data",
        python_callable=verify_data,
    )

    end = EmptyOperator(task_id="end")

    # Dependencies
    start >> create_table_task >> extract_task >> transform_task >> load_task >> verify_task >> end