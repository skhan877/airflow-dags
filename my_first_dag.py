from airflow import DAG 
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime 
from random import randint 


def _training_model():
    return randint(0, 10)

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=["model_A", "model_B", "model_C"])
    best_accuracy = max(accuracies)
    return "accurate" if best_accuracy >= 8 else "inaccurate" 

with DAG(
    dag_id="randint_dag"
    , start_date=datetime(2025, 1, 1)
    , schedule_interval="@daily"
    , catchup=False
    ) as dag:

        model_A = PythonOperator(
            task_id="model_A"
            , python_callable=_training_model
        )

        model_B = PythonOperator(
            task_id="model_B"
            , python_callable=_training_model
        )

        model_C = PythonOperator(
            task_id="model_C"
            , python_callable=_training_model
        )

        choose_best_model = BranchPythonOperator(
            task_id="choose_best_model"
            , python_callable=_choose_best_model
        )

        accurate = BashOperator(
            task_id="accurate"
            , bash_command="echo 'accurate'"
        )        
        
        inaccurate = BashOperator(
            task_id="inaccurate"
            , bash_command="echo 'inaccurate'"
        )

        [model_A, model_B, model_C] >> choose_best_model >> [accurate, inaccurate]