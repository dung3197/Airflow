from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Define the callback function
def report_dag_success(context):
    dag_run = context.get('dag_run')
    start_time = dag_run.start_date
    end_time = dag_run.end_date
    duration = end_time - start_time
    
    # Log the information
    logging.info(f"DAG {dag_run.dag_id} completed successfully.")
    logging.info(f"Start Time: {start_time}")
    logging.info(f"End Time: {end_time}")
    logging.info(f"Duration: {duration}")

# Define a sample task
def sample_task():
    print("This is a sample task.")

# Define the DAG
with DAG(
    dag_id='report_dag_duration',
    start_date=datetime(2025, 5, 12),
    schedule_interval=None,
    on_success_callback=report_dag_success,
    catchup=False
) as dag:
    
    task = PythonOperator(
        task_id='sample_task',
        python_callable=sample_task
    )
