# import airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# import local modules
from crud import load_data

from datetime import datetime




dag = DAG(
    dag_id="extract_load_101",
    schedule_interval= '*/10 * * * *',
    start_date = datetime(2022,8,6),
    catchup = False,
)

def run_exctract_load():

    load_data()
command_01 = """python3 transaction/run.py"""
extract_load = BashOperator(task_id = 'extract_load_task',
                            bash_command=command_01,
                            dag=dag,
                            )

command_02 = """python3 production/test_run.py"""
load_product  = BashOperator(task_id='load_product_task',
                             bash_command= command_02,
                            dag = dag)

def display_log():
    print("Extract Load Task Done!!!")

notify = PythonOperator(task_id='notify',
                        python_callable=display_log,
                        dag=dag,
                        )


extract_load >> load_product >> notify