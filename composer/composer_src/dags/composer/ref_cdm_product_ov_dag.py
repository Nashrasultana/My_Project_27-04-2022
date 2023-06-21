
import datetime
from datetime import timedelta
import os
from airflow import models
from airflow.models import BaseOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator

from airflow.models import Variable

dag_config = Variable.get('ref_cdm_product_ov_dag_variables', deserialize_json=True, default_var={})
product_ov_load_sql_statement = Variable.get('product_ov_load_sql', default_var={})
product_ov_timestamp_sql_statement = Variable.get('product_ov_timestamp_sql', default_var={})

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
    'start_date': datetime.datetime(2022, 6, 14),
    "location": gce_region,
    "serviceAccountEmail": composer_sa_email
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        dag_id='ref_cdm_product_ov_dag',
        schedule_interval=schedule_interval,
        max_active_runs=1,
        catchup=False,
        tags=['3gss'],
        default_args=default_args) as dag:

    Clear_table = BigQueryExecuteQueryOperator(
        task_id="Clear_table",
        sql=""" delete from `bt-tch-3g-sunset-dp-prod.dp_net_mobnet_rw.ref_cdm_product_ov` where 1=1 """,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )

    # BQtoBQ_load_job = BigQueryExecuteQueryOperator(
    #     task_id="BQtoBQ_load_job",
    #     sql=""" insert into `bt-tch-3g-sunset-dp-prod.dp_net_mobnet_rw.ref_cdm_product_vo`(product_code,effective_date,expiration_date,protocol,product_rollup,base_type,product_group,product_description,total_allowance_data,family_type,base_type_level1,product_type,stack,brand) select product_code,effective_date,expiration_date,protocol,product_rollup,base_type,product_group,product_description,total_allowance_data,family_type,base_type_level1,product_type,stack,brand from `bt-con-fincdmi-dp-prod.con_dp_cdm_ref_ro.ref_cdm_product_vo` """,
    #     write_disposition="WRITE_TRUNCATE",
    #     gcp_conn_id=GCP_DEFAULT_CONN,
    #     use_legacy_sql=False,
    # )
    
    BQtoBQ_load_job = BigQueryExecuteQueryOperator(
        task_id="BQtoBQ_load_job",
        sql=product_ov_load_sql_statement,
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )

    # insert_timestamp = BigQueryExecuteQueryOperator(
    #     task_id="insert_timestamp",
    #     sql=""" update `bt-tch-3g-sunset-dp-prod.dp_net_mobnet_rw.ref_cdm_product_vo` set record_ts = current_timestamp() where record_ts IS NULL """,
    #     write_disposition="WRITE_APPEND",
    #     gcp_conn_id=GCP_DEFAULT_CONN,
    #     use_legacy_sql=False,
    # )

    insert_timestamp = BigQueryExecuteQueryOperator(
        task_id="insert_timestamp",
        sql=product_ov_timestamp_sql_statement,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )
    
    Clear_table >> BQtoBQ_load_job >> insert_timestamp
