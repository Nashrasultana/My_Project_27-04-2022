import datetime
from datetime import timedelta
import airflow
from airflow import DAG
from airflow import models
from airflow.models import BaseOperator
from airflow.operators import bash
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from google.cloud import storage

dag_config = Variable.get('var_3g_import_dag_variables_config', deserialize_json=True, default_var={})
project_id = dag_config.get("project_id")
gce_zone = dag_config.get("gce_zone")
gce_region = dag_config.get("gce_region")
schedule_interval = dag_config.get('schedule_interval')
composer_sa_email = dag_config.get('df_sa_email')
prefix = dag_config.get('prefix') 
bucket_name = dag_config.get('bucket_name') 

storage_client = storage.Client()
GCP_DEFAULT_CONN = 'google_cloud_default'
list_files=storage_client.list_blobs(bucket_name,prefix=prefix)

default_args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2022, 6, 14),
    "location": gce_region,
    "serviceAccountEmail": composer_sa_email
}

with models.DAG(
        dag_id='Generic_dag_import_airflow_variables',
        schedule_interval=None,
        max_active_runs=1,
        catchup=False,
        tags=['3gss'],
        default_args=default_args) as dag:

    for file in list_files:
        num=file.name.count("/")
        filename=file.name.split('/')[num]
        command=f"gcloud composer environments run composer-dev-two-dev --project=bt-dig-netcompose-cc-dev-ops --location=europe-west2  variables -- import /home/airflow/gcs/data/dag_variables/"+filename

        run_airflow_test = BashOperator(
                task_id='test_task'+filename,
                bash_command=command,
                dag=dag,
                depends_on_past=False,
                do_xcom_push=False
        )
        run_airflow_test
