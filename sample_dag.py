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













from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def get_dag1_conf(context, dag_run_obj):
    conf = context['dag_run'].conf or {}
    dag_run_obj.payload = conf.get('dag1_conf', {})
    return dag_run_obj

task1 = TriggerDagRunOperator(
    task_id="trigger_dag1",
    trigger_dag_id="dag1",
    execution_date="{{ execution_date }}",  # optional, can omit if not needed
    python_callable=get_dag1_conf
)
































from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="manager_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    trigger_task1 = TriggerDagRunOperator(
        task_id="trigger_dag1",
        trigger_dag_id="dag1",
        conf="{{ dag_run.conf.get('dag1_conf', {}) }}"
    )

    trigger_task2 = TriggerDagRunOperator(
        task_id="trigger_dag2",
        trigger_dag_id="dag2",
        conf="{{ dag_run.conf.get('dag2_conf', {}) }}"
    )

    trigger_task3 = TriggerDagRunOperator(
        task_id="trigger_dag3",
        trigger_dag_id="dag3",
        conf="{{ dag_run.conf.get('dag3_conf', {}) }}"
    )

    trigger_task1 >> trigger_task2 >> trigger_task3



















from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

def extract_dag1_conf(**kwargs):
    dag1_conf = kwargs['dag_run'].conf.get('dag1_conf', {})
    kwargs['ti'].xcom_push(key='conf', value=dag1_conf)

with DAG(
    dag_id='manager_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    prepare_task1 = PythonOperator(
        task_id='prepare_dag1_conf',
        python_callable=extract_dag1_conf,
        provide_context=True
    )

    trigger_task1 = TriggerDagRunOperator(
        task_id='trigger_dag1',
        trigger_dag_id='dag1',
        conf="{{ ti.xcom_pull(task_ids='prepare_dag1_conf', key='conf') }}"
    )

    # Repeat for dag2
    def extract_dag2_conf(**kwargs):
        dag2_conf = kwargs['dag_run'].conf.get('dag2_conf', {})
        kwargs['ti'].xcom_push(key='conf', value=dag2_conf)

    prepare_task2 = PythonOperator(
        task_id='prepare_dag2_conf',
        python_callable=extract_dag2_conf,
        provide_context=True
    )

    trigger_task2 = TriggerDagRunOperator(
        task_id='trigger_dag2',
        trigger_dag_id='dag2',
        conf="{{ ti.xcom_pull(task_ids='prepare_dag2_conf', key='conf') }}"
    )

    # Repeat for dag3
    def extract_dag3_conf(**kwargs):
        dag3_conf = kwargs['dag_run'].conf.get('dag3_conf', {})
        kwargs['ti'].xcom_push(key='conf', value=dag3_conf)

    prepare_task3 = PythonOperator(
        task_id='prepare_dag3_conf',
        python_callable=extract_dag3_conf,
        provide_context=True
    )

    trigger_task3 = TriggerDagRunOperator(
        task_id='trigger_dag3',
        trigger_dag_id='dag3',
        conf="{{ ti.xcom_pull(task_ids='prepare_dag3_conf', key='conf') }}"
    )

    # Set task dependencies
    prepare_task1 >> trigger_task1 >> prepare_task2 >> trigger_task2 >> prepare_task3 >> trigger_task3
































from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

def verify_and_decide(**context):
    config = Variable.get("manual_rerun", default_var='{}', deserialize_json=True)
    rerun_task = config.get("rerun_task")
    params = config.get("params", {})

    # Push to XCom so downstream tasks can access this info
    context['ti'].xcom_push(key="rerun_task", value=rerun_task)
    context['ti'].xcom_push(key="params", value=params)

    print(f"Will rerun only task: {rerun_task}, with params: {params}")

def conditional_execute(task_name):
    def _inner(**context):
        ti = context["ti"]
        rerun_task = ti.xcom_pull(task_ids="verify_and_decide", key="rerun_task")
        params = ti.xcom_pull(task_ids="verify_and_decide", key="params", default={})

        if rerun_task != task_name:
            print(f"Skipping {task_name} because it's not the target")
            return False  # ShortCircuit this task

        print(f"Running {task_name} with params: {params}")
        return True
    return _inner

with DAG(
    dag_id="conditional_rerun_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    verify = PythonOperator(
        task_id="verify_and_decide",
        python_callable=verify_and_decide,
        provide_context=True
    )

    task1 = ShortCircuitOperator(
        task_id="task1",
        python_callable=conditional_execute("task1"),
        provide_context=True
    )

    task2 = ShortCircuitOperator(
        task_id="task2",
        python_callable=conditional_execute("task2"),
        provide_context=True
    )

    verify >> [task1, task2]
