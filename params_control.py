from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the manager DAG
with DAG(
    'manager_dag',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger or external trigger
    default_args=default_args,
    catchup=False,
) as dag:

    def get_dag_conf(context, dag_id):
        """Extract configuration for a specific DAG from dag_run.conf."""
        dag_run_conf = context['dag_run'].conf if context['dag_run'] else {}
        # Default parameters for each DAG if not provided
        default_conf = {
            'dag1': {'param1': 'default1', 'param2': 'default2'},
            'dag2': {'param3': 'default3', 'param4': 'default4'},
            'dag3': {'param5': 'default5', 'param6': 'default6'},
        }
        # Merge default config with provided config (if any)
        return dag_run_conf.get(dag_id, default_conf[dag_id])

    # Task to trigger dag1
    task1 = TriggerDagRunOperator(
        task_id='trigger_dag1',
        trigger_dag_id='dag1',
        conf="{{ ti.xcom_pull(task_ids='get_dag1_conf') }}",
        dag=dag,
    )

    # Task to trigger dag2
    task2 = TriggerDagRunOperator(
        task_id='trigger_dag2',
        trigger_dag_id='dag2',
        conf="{{ ti.xcom_pull(task_ids='get_dag2_conf') }}",
        dag=dag,
    )

    # Task to trigger dag3
    task3 = TriggerDagRunOperator(
        task_id='trigger_dag3',
        trigger_dag_id='dag3',
        conf="{{ ti.xcom_pull(task_ids='get_dag3_conf') }}",
        dag=dag,
    )

    # Tasks to prepare configuration for each DAG
    def prepare_dag1_conf(**context):
        conf = get_dag_conf(context, 'dag1')
        return conf

    def prepare_dag2_conf(**context):
        conf = get_dag_conf(context, 'dag2')
        return conf

    def prepare_dag3_conf(**context):
        conf = get_dag_conf(context, 'dag3')
        return conf

    get_dag1_conf = PythonOperator(
        task_id='get_dag1_conf',
        python_callable=prepare_dag1_conf,
        provide_context=True,
        dag=dag,
    )

    get_dag2_conf = PythonOperator(
        task_id='get_dag2_conf',
        python_callable=prepare_dag2_conf,
        provide_context=True,
        dag=dag,
    )

    get_dag3_conf = PythonOperator(
        task_id='get_dag3_conf',
        python_callable=prepare_dag3_conf,
        provide_context=True,
        dag=dag,
    )

    # Define task dependencies
    get_dag1_conf >> task1 >> get_dag2_conf >> task2 >> get_dag3_conf >> task3
