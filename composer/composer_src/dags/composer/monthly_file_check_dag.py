import datetime
from datetime import timedelta
import os
from airflow import models
from airflow.models import BaseOperator
from airflow.operators import bash
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.exceptions import AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.models import Variable

def gcs_file_validation(ti):
    from google.cloud import logging
    from google.cloud import bigquery
    from google.cloud import storage

    client = logging.Client()
    client.setup_logging()
    bq_client = bigquery.Client()
    storage_client=storage.Client()

    result_dict=dict()

    table=bq_client.get_table("bt-tch-3g-sunset-dp-dev.net_mobnet_rw.t_agg_mth_subscriber_usage")

    get_latest_month_job=bq_client.query("select property_value from `bt-tch-3g-sunset-dp-dev.mobnet_ops_ro.mobnet_system_properties` where property_name='latest_mthly_file_mon'")
    result_set=get_latest_month_job.result()
    for row in result_set:
        latest_month=str(row.property_value)
        result_dict['latest_month']=latest_month
    print("latest month data loaded in 3g system is ",latest_month)

    get_next_month_job=bq_client.query("select property_value from `bt-tch-3g-sunset-dp-dev.mobnet_ops_ro.mobnet_system_properties` where property_name='next_mthly_file_mon'")
    result_set_next=get_next_month_job.result()

    for row in result_set_next:
      next_month=str(row.property_value) 
      result_dict['next_month']=next_month
      ti.xcom_push(key='next_month', value=next_month)
    print("\n This execution will load data for month ",next_month)

    initial_record_cnt=table.num_rows
    result_dict['initial_record_cnt']=initial_record_cnt
    ti.xcom_push(key='initial_record_cnt', value=initial_record_cnt)

    print("\n Before execution record count in main table is ",initial_record_cnt)

    bucket_name=storage_client.get_bucket("bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage")
    prefix="EE_T_AGG_MTH_SUBSCRIBER_USAGE_"+next_month
    print ("\n prefix to search in bucket",prefix)

    hash_file_prefix="EE_T_AGG_MTH_SUBSCRIBER_USAGE_"+next_month+"_006_016_HASH.dat"
    hash_blob=bucket_name.blob(hash_file_prefix)
    download_has_file=hash_blob.download_as_text(encoding="utf-8")
    
    with hash_blob.open("r") as f:
      
        records_to_be_loaded_cnt=f.readlines()[-1]
        result_dict['records_to_be_loaded_cnt']=records_to_be_loaded_cnt
        ti.xcom_push(key='records_to_be_loaded_cnt', value=records_to_be_loaded_cnt)
        print("\n Total number of records loaded in this month is",records_to_be_loaded_cnt)
        
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    file_count = sum(1 for _ in blobs)
    result_dict['total_file_count']=file_count
    print("\n file count available to load",file_count)

    print(result_dict)

    ti.xcom_push(key='total_file_count', value=file_count)
    return result_dict
    






def check_count(ti):
    dt = ti.xcom_pull(task_ids=['gcs_file_validation'],key='total_file_count')[0]
    
    if int(dt) >= 102:
        return True
        return False

def bq_table_load_validation(ti):
    from google.cloud import logging
    from google.cloud import bigquery
    from google.cloud import storage

    client = logging.Client()
    client.setup_logging()
    bq_client = bigquery.Client()
    storage_client=storage.Client()

    table=bq_client.get_table("bt-tch-3g-sunset-dp-dev.net_mobnet_rw.t_agg_mth_subscriber_usage")

    final_record_cnt=table.num_rows
    initial_record_cnt=ti.xcom_pull(task_ids=['gcs_file_validation'],key='initial_record_cnt')[0]
    Total_rec_to_be_loaded=ti.xcom_pull(task_ids=['gcs_file_validation'],key='records_to_be_loaded_cnt')[0]

    if final_record_cnt-initial_record_cnt == Total_rec_to_be_loaded:
        print("\n Biq query load process validation failed,difference in record loaded and record recieved in monthly file",final_record_cnt-initial_record_cnt)
        return False
    else:
        print("\n Biq query load process validation is successful")
        return True

     










dag_config = Variable.get('monthly_file_check_dag_variables', deserialize_json=True, default_var={})

project_id = dag_config.get("project_id")
gce_zone = dag_config.get("gce_zone")
gce_region = dag_config.get("gce_region")
schedule_interval = dag_config.get('schedule_interval')
composer_sa_email = dag_config.get('df_sa_email')
schedule_interval = dag_config.get('schedule_interval')
bucket_path = dag_config.get("bucket_path")
raw_bucket_name = dag_config.get("raw_bucket_name")
input_file = dag_config.get("input_file")
hash_file = dag_config.get("hash_file")
bq_project_id = dag_config.get("bq_project_id")
bq_output_dataset = dag_config.get("bq_output_dataset")
bq_output_table = dag_config.get("bq_output_table")

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
        dag_id='monthly_file_check_dag',
        schedule_interval=schedule_interval,
        max_active_runs=1,
        catchup=False,
        default_args=default_args) as dag:

 #   list_files = bash.BashOperator(
 #       task_id="list_files",
 #       bash_command=f"gsutil du gs://bt-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE_T_AGG_MTH*.gz | wc -l",
 #       do_xcom_push=True,
 #   )

    gcs_validation = PythonOperator(
        task_id='gcs_file_validation',
        provide_context=True,
        python_callable=gcs_file_validation,
        do_xcom_push=True
    )

    check_file_count = ShortCircuitOperator(
        task_id='check_count',
        provide_context=True,
        python_callable=check_count,

    )

    trigger_agg_mth_dag = TriggerDagRunOperator(
        task_id="trigger_agg_mth_dag",
         trigger_dag_id="t_agg_mth_subscriber_usage_dag",
        wait_for_completion=True
    )  

     bq_load_validation = ShortCircuitOperator(
        task_id='bq_load_validation',
        provide_context=True,
        python_callable=bq_table_load_validation,

    )  

    trigger_bi_ccm_dag = TriggerDagRunOperator(
        task_id="trigger_bi_ccm_dag",
        trigger_dag_id="bi_ccm_agg_mth_dag",
        wait_for_completion=True
    )    
    trigger_best_device_dag = TriggerDagRunOperator(
        task_id="trigger_best_device_dag",
        trigger_dag_id="mth_best_device_dag",
        wait_for_completion=True
    )    
    trigger_usage_agg_dag = TriggerDagRunOperator(
        task_id="trigger_usage_agg_dag",
        trigger_dag_id="usage_agg_dag",
        wait_for_completion=True
    )    
    

    gcs_validation >> check_file_count >> trigger_agg_mth_dag >> bq_load_validation >> trigger_bi_ccm_dag >> trigger_best_device_dag >> trigger_usage_agg_dag
