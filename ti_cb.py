from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from airflow.utils.state import State
from datetime import datetime

def push_task_metric(context):
    """
    Push a single task's metric to Pushgateway.
    """
    ti = context['ti']
    task_id = ti.task_id
    status = ti.state or "unknown"
    timestamp = str(ti.end_date or datetime.utcnow())

    registry = CollectorRegistry()

    # Build labels with dynamic label key (task name)
    labels = {
        'Task': task_id,
        'timestamps': timestamp,
        task_id: status  # Dynamic label key
    }

    g = Gauge(
        'TI_stats',
        'Airflow Task Stats',
        list(labels.keys()),
        registry=registry
    )

    state_map = {
        State.SUCCESS: 1,
        State.FAILED: 2,
        State.UP_FOR_RETRY: 3,
        State.RUNNING: 4,
        State.SKIPPED: 5,
        'unknown': 0
    }

    g.labels(**labels).set(state_map.get(status, 0))

    push_to_gateway(
        'your-pushgateway-host:9091',
        job=f"airflow_task_{ti.dag_id}",
        registry=registry
    )
def push_dag_metric(context):
    """
    Push DAG run metric to Pushgateway.
    """
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    status = dag_run.get_state() or "unknown"
    timestamp = str(dag_run.end_date or datetime.utcnow())

    registry = CollectorRegistry()

    labels = {
        'DAG': dag_id,
        'timestamps': timestamp,
        dag_id: status  # Dynamic label key
    }

    g = Gauge(
        'DAG_stats',
        'Airflow DAG Stats',
        list(labels.keys()),
        registry=registry
    )

    state_map = {
        State.SUCCESS: 1,
        State.FAILED: 2,
        State.RUNNING: 3,
        'unknown': 0
    }

    g.labels(**labels).set(state_map.get(status, 0))

    push_to_gateway(
        'your-pushgateway-host:9091',
        job=f"airflow_dag_{dag_id}",
        registry=registry
    )
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'on_success_callback': push_task_metric,
    'on_failure_callback': push_task_metric
}

with DAG(
    dag_id='example_metrics_dag',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    on_success_callback=push_dag_metric,
    on_failure_callback=push_dag_metric,
    catchup=False
) as dag:

    def example(**kwargs):
        print("Hello from task")

    task1 = PythonOperator(
        task_id='Task1',
        python_callable=example,
        provide_context=True
    )

    task2 = PythonOperator(
        task_id='Task2',
        python_callable=example,
        provide_context=True
    )

    task1 >> task2
