from airflow import DAG
#Airflow 1
#from airflow.operators import  BashOperator
#Airflow 2
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


with DAG(
dag_id='Pubsub_push',
start_date=days_ago(1),
schedule_interval = '*/2 * * * *',
catchup=False
) as dag:
    t1 = BashOperator(
        task_id='UplinkService',
        bash_command='python /home/airflow/gcs/dags/uplink.py',
        dag=dag)