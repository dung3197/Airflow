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




















from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="manager_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    task1 = TriggerDagRunOperator(
        task_id="trigger_dag1",
        trigger_dag_id="dag1",
        conf="{{ dag_run.conf.get('dag1_conf', {}) }}"
    )

    task2 = TriggerDagRunOperator(
        task_id="trigger_dag2",
        trigger_dag_id="dag2",
        conf="{{ dag_run.conf.get('dag2_conf', {}) }}"
    )

    task3 = TriggerDagRunOperator(
        task_id="trigger_dag3",
        trigger_dag_id="dag3",
        conf="{{ dag_run.conf.get('dag3_conf', {}) }}"
    )

    task1 >> task2 >> task3




{
  "dag1_conf": {"param_a": "value1"},
  "dag2_conf": {"param_b": "value2"},
  "dag3_conf": {"param_c": "value3"}
}










from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def run_task(**kwargs):
    conf = kwargs.get('dag_run').conf or {}
    param = conf.get('param_a', 'default_value')
    print(f"Running with param_a: {param}")

with DAG(
    dag_id="dag1",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="print_param",
        python_callable=run_task,
        provide_context=True
    )
