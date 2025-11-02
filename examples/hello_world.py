from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime

def greet():
    print("Hello from Airflow, running in WSL2!")

with DAG(
    dag_id="hello_world"
    , start_date=datetime(2025,1,1)
    , schedule_interval="@daily"
    , catchup=False 
) as dag:

    PythonOperator(
        task_id="task_say_hello"
        , python_callable=greet
    )
