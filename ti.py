from airflow.models import TaskInstance
from airflow.utils.session import create_session
from datetime import datetime

def report_task_info(context):
    dag_run = context['dag_run']
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    print(f"\n📋 Task Execution Info for DAG: {dag_id} | Run: {run_id}\n")

    with create_session() as session:
        task_instances = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id
        ).all()

        for ti in task_instances:
            start = ti.start_date
            end = ti.end_date
            duration = (end - start).total_seconds() if start and end else None

            print(f"🔹 Task ID     : {ti.task_id}")
            print(f"   State       : {ti.state}")
            print(f"   Start Time  : {start}")
            print(f"   End Time    : {end}")
            print(f"   Duration    : {duration:.2f} seconds" if duration else "   Duration    : N/A")
            print("-" * 40)
