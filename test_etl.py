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

def transform_data(users): 
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

def load_data(records):
    df = pd.DataFrame(records)
    output_path = os.path.expanduser("~/airflow/data/fake-users.csv")
    os.mkdirs(os.path.dirname(output_path), exist_ok=True)
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
        task_id="extract",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        op_args=["{{ ti.xcom_pull(task_ids='extract') }}"]
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data,
        op_args=["{{ ti.xcom_pull(task_ids='transform') }}"]
    )

    extract >> transform >> load 
