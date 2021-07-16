'''
Source:
https://cloud.google.com/composer/docs/how-to/using/triggering-with-gcf
https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/example_dags/example_gcs_to_bigquery.html
https://guptakumartanuj.wordpress.com/2018/01/21/passing-and-accessing-run-time-arguments-to-dag-airflow-through-cli/
https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
https://github.com/tuanchris/cloud-data-lake/blob/master/dags/cloud-data-lake-pipeline.py
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os
from airflow.models import Variable

DATASET_NAME = Variable.get("GCP_DATASET_NAME")
TABLE_NAME = Variable.get("GCP_TABLE_NAME")
Project_id = 'egencloud'

with DAG(
    dag_id='Push_to_BigQuery',
    start_date=days_ago(1),
    schedule_interval=None) as dag:

    # Print the received dag_run configuration.
    # The DAG run configuration contains information about the
    # Cloud Storage object change.
    t1 = BashOperator(
        task_id='print_gcs_info',
        bash_command="echo Bucket_name: {{ dag_run.conf['bucket']}} and File Name: {{dag_run.conf['name']}} ",
        dag=dag)

#Source object Type List
#Write MEthods: WRITE_APPEND,WRITE_TRUNCATE
    load_to_BQ = GCSToBigQueryOperator(
        task_id='load_to_BQ',
        bucket="{{ dag_run.conf['bucket']}}",
        source_objects = ["{{dag_run.conf['name']}}"],
        destination_project_dataset_table=f'{Project_id}:{DATASET_NAME}.{TABLE_NAME}',
        schema_fields=[ {'name':'startTime','type':'TIMESTAMP','mode': 'NULLABLE'},
        {'name':'temperature','type':'INTEGER','mode': 'NULLABLE'}, 
        {'name':'windDirection','type':'STRING','mode': 'NULLABLE'},
         {'name':'shortForecast','type':'STRING','mode': 'NULLABLE'}, 
         {'name':'windSpeed','type':'INTEGER','mode': 'NULLABLE'}
        ],
        #autodetect = True,
         write_disposition='WRITE_APPEND',
         source_format = 'csv',
          skip_leading_rows = 1,
          dag=dag)
t1 >> load_to_BQ



   