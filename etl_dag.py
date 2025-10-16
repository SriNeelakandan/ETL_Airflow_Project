from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore
from datetime import datetime, timedelta

config={
    'owner':'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=1),
}

new_dag= DAG(
    'new_dag', # DAG NAME
    default_args=config,
    description= 'ETL that is used for Sales data',
    schedule_interval=timedelta(minutes=5),
    start_date =datetime(2025,10,16),
    catchup=False
)

run_etl = BashOperator(
    task_id='run_etl_tablet_record',
    bash_command ='bash /home/neels/wrapper_script.sh ',
    dag =new_dag,
)


