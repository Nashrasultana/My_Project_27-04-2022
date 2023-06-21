import datetime
from datetime import timedelta
import os
from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator

from airflow.models import Variable

dag_config = Variable.get('network_dashboard_3month_data_dag_variables', deserialize_json=True, default_var={})
network_dashboard_3month_sql_statement = Variable.get('network_dashboard_3month_data_sql', default_var={})

project_id = dag_config.get("project_id")
gce_zone = dag_config.get("gce_zone")
gce_region = dag_config.get("gce_region")
schedule_interval = dag_config.get('schedule_interval')
composer_sa_email = dag_config.get('df_sa_email')

GCP_DEFAULT_CONN = 'google_cloud_default'

default_args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2023, 4, 13),
    "location": gce_region,
    "serviceAccountEmail": composer_sa_email
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.

with models.DAG(
        dag_id='network_dashboard_3month_data_dag',
        schedule_interval=schedule_interval,
        max_active_runs=1,
        catchup=False,
        tags=['3gss'],
        default_args=default_args) as dag:
    
    BQtoBQ_Load_job = BigQueryExecuteQueryOperator(
        task_id="BQtoBQ_Load_job",
        sql=network_dashboard_3month_sql_statement,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )
    
    BQtoBQ_Load_job