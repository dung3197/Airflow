from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

#firstly define default arguments like owner name,dag start_date ,etc....
args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 26),
    'retries': 2,
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=5),
    }

#Creating Main DAG with help of With Clause
with DAG("Simple_TaskGroup_Example",default_args=args) as dag:

    start=DummyOperator(task_id="Start")
    end=DummyOperator(task_id="End")
    
    
    with TaskGroup("Section_1",tooltip="TASK_GROUP_EV_description") as Section_1:

        t1=BashOperator(
                task_id="Section-1-Task-1",
                bash_command='echo "Section-1-Task-1"'
                )
        t2=BashOperator(
                task_id="Section-1-Task-2",
                bash_command='echo "Section-1-Task-2"'
                )

    with TaskGroup("Section_2",tooltip="TASK_GROUP_EV_description") as Section_2:
        
        t1=BashOperator(
                task_id="Section-2-Task-1",
                bash_command='echo "Section-2-Task-1"'
                )
        t2=BashOperator(
                task_id="Section-2-Task-2",
                bash_command='echo "Section-2-Task-2"'
                )
        
        
#serially run TaskGroup DAG in following dependencies
#start>>Section_1>>Section_2>>end

#Parallel run TaskGroup DAG in following dependencies
start>>Section_1>>end
start>>Section_2>>end
