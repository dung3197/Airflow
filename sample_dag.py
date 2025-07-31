from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dag3',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Triggered by manager_dag
    default_args=default_args,
    catchup=False,
) as dag:

    def process(**context):
        # Access the configuration passed from manager_dag
        conf = context['dag_run'].conf if context['dag_run'] else {}
        param5 = conf.get('param5', 'default5')
        param6 = conf.get('param6', 'default6')
        print(f"Running dag3 with param5={param5}, param6={param6}")

    process_task = PythonOperator(
        task_id='process',
        python_callable=process,
        provide_context=True,
        dag=dag,
    )
