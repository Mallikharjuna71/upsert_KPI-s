from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

# Function for the first DAG task
def task_1_func(**kwargs):
    print("Task 1 is executing.")

# Function for the second DAG task
def task_2_func(**kwargs):
    print(f"Task 2 is executing with the result from DAG 1")

# Default arguments for the DAGs
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 1 definition
dag1 = DAG(
    'dag1',
    default_args=default_args,
    description='First DAG in the sequence',
    schedule_interval='cron expression',  # This DAG is triggered manually
    start_date=datetime(2025, 2, 24),
    catchup=False,
)

# Define the tasks for DAG 1
start_task_1 = DummyOperator(
    task_id='start_task_1',
    dag=dag1,
)

task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1_func,
    provide_context=True,
    dag=dag1,
)

# Task dependencies for DAG 1
start_task_1 >> task_1

# DAG 2 definition
dag2 = DAG(
    'dag2',
    default_args=default_args,
    description='Second DAG in the sequence, dependent on DAG 1',
    schedule_interval=None,  # This DAG is triggered manually
    start_date=datetime(2025, 2, 24),
    catchup=False,
)

# ExternalTaskSensor to wait for DAG 1 to complete successfully
wait_for_dag1 = ExternalTaskSensor(
    task_id='wait_for_dag1',
    external_dag_id='dag1',  # The DAG you are waiting for
    external_task_id='task_1',  # The task within the DAG you're waiting for
    allowed_status = ['success']
    mode='poke',  # You can use 'poke' or 'reschedule' mode
    timeout=600,  # Wait for a max of 10 minutes
    poke_interval=60,  # Check every minute
    dag=dag2
    )


# Define the task for DAG 2
task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2_func,
    provide_context=True,
    dag=dag2,
)

# Task dependencies for DAG 2
wait_for_dag1 >> task_2
