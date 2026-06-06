from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for the DAG tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello_func():
    print("Hello from Apache Airflow v3.2.2! Your containerized from-scratch setup is working perfectly.")
    return "Verification Successful"

# Define the DAG context
with DAG(
    'verification_test_dag',
    default_args=default_args,
    description='A simple test DAG to verify Airflow setup and task execution',
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['example', 'test'],
) as dag:

    # Task 1: Print Hello using PythonOperator
    task_hello = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello_func,
    )

    # Task 2: Print Date using BashOperator
    task_date = BashOperator(
        task_id='print_date_task',
        bash_command='date',
    )

    # Task dependency
    task_hello >> task_date
