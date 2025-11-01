import pandas as pd 
import requests
import os 
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator



def extract_data():
    url = "https://randomuser.me/api/?results=10"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json() 
    return data["results"]

def transform_data(**context):
    ti = context["ti"]
    users = ti.xcom_pull(task_ids="extract_data") 

    transformed_data = [] 
    for user in users:
        transformed_data.append({
            "first_name": user["name"]["first"],
            "last_name": user["name"]["last"],
            "email": user["email"],
            "country": user["location"]["country"]
        })
    df = pd.DataFrame(transformed_data)
    return df.to_dict(orient="records")

def load_data(**context):
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="transform_data")
    df = pd.DataFrame(records)
    output_path = os.path.expanduser("~/airflow/data/fake-users.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path} with {len(df)} rows.")


with DAG(
    dag_id="etl_users_api_to_csv",
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "etl"]
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True
    )

    extract >> transform >> load 
