import datetime
import os
from datetime import timedelta
from google.cloud import storage
from airflow import models
from airflow.operators import bash
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

  

dag_config = Variable.get('t_agg_dly_subs_usage_dag_variables', deserialize_json=True, default_var={})
t_agg_dly_load_sql_statement = Variable.get('t_agg_dly_subs_load_sql', default_var={})
t_agg_dly_timestamp_sql_statement = Variable.get('t_agg_dly_subs_timestamp_sql', default_var={})

bucket_path = dag_config.get("bucket_path")
raw_bucket_name = dag_config.get("raw_bucket_name")
input_file = dag_config.get("input_file")
temp_file_name =dag_config.get("temp_file_name")
project_id = dag_config.get("project_id")
bq_project_id = dag_config.get("bq_project_id")
bq_output_dataset = dag_config.get("bq_output_dataset")
bq_output_table = dag_config.get("bq_output_table")
gce_zone = dag_config.get("gce_zone")
output = dag_config.get("output")
gce_region = dag_config.get("gce_region")
schedule_interval = dag_config.get('schedule_interval')
df_job_name = dag_config.get('df_job_name')
df_sa_email = dag_config.get('df_sa_email')
df_subnet = dag_config.get('df_subnet')
df_template = dag_config.get('df_template')
clean_output_bucket = dag_config.get('clean_output_bucket')
agg_dly_destination_dataset = dag_config.get('agg_dly_destination_dataset')
output_file_header = dag_config.get('output_file_header')
input_file_headers = dag_config.get('input_file_headers')
primary_key = dag_config.get('primary_key')
columns_to_remove = dag_config.get('columns_to_remove')




GCP_DEFAULT_CONN = 'google_cloud_default'

def file_validation(ti) -> None:
    dt = ti.xcom_pull(task_ids=['list_files'])
    if not dt:
        raise ValueError('No value currently stored in XComs.')

    print('The value of xcom is',dt)
    

    if int(dt[0]) >= 1: 
        print('Number of files recieved is greater than 1')
    else:
        raise AirflowException('Files have not arrived yet') 

default_args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2021, 5, 19),
    "location": gce_region,
    "dataflow_default_options": {
        "project": project_id,
        #"region": gce_region,
        "zone": gce_zone,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_path + "/tmp/",
        "serviceAccountEmail": df_sa_email,
        "subnetwork": df_subnet
    },
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        dag_id='t_agg_dly_subs_usage_dag',
        schedule_interval=schedule_interval,
        max_active_runs=1,
        catchup=False,
        tags=['3gss'],
        default_args=default_args) as dag:

    list_files = bash.BashOperator(
        task_id="list_files",
        bash_command=f"gsutil ls -l gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-dly-subscriber-usage/EE_T_AGG_DLY*.gz | wc -l",
        do_xcom_push=True,
    )

    save_file_count = PythonOperator(
        task_id='save_file_count',
        python_callable=file_validation
    )

    # gcs_object_exists = GCSObjectExistenceSensor(
    #     bucket='bt-dev-raw-ccn-net-mobnet-edwh-agg-dly-subs-usage',
    #     object='EE_DIM_CELL_DATA*.gz',
    #     task_id="gcs_object_exists_task",
    #     timeout= 130
    # )
    
    # start_template_job = DataflowTemplateOperator(
    #     # The task id of your job
    #     task_id="dataflow_operator_t_agg_dly_subscriber_usage_batch",
    #     template=df_template,
    #     # Use the link above to specify the correct parameters for your template.
    #     parameters={
    #         "input": bucket_path + "/" + input_file,
    #         "output": output 
    #     },
    #     gcp_conn_id=GCP_DEFAULT_CONN,
    #     job_name=df_job_name + '-' + datetime.datetime.now().strftime("%Y%m%d%H")
    # )

    start_gcs_to_bq_job = GCSToBigQueryOperator(
        task_id='gcs_to_bq_agg_dly',
        bucket=raw_bucket_name,
        source_objects=[input_file],
        destination_project_dataset_table=agg_dly_destination_dataset,
        schema_fields=[
            {'name': 'date_of_call', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'msisdn', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'exbr_account_num', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'css_account_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'jt_customer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},    
            {'name': 'subscription_sk', 'type': 'INTEGER', 'mode': 'NULLABLE'},        
            {'name': 'imei', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'imsi', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_cell_data_id', 'type': 'INTEGER','mode': 'NULLABLE'},
            {'name': 'last_cell_data_id', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mo_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'home_mt_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'home_mo_sms_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'home_mt_sms_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mo_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'home_mt_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'home_data_cdr_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'home_data_in_volume_kb', 'type': 'INTEGER','mode': 'NULLABLE'},
            {'name': 'work_mo_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mt_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'work_mo_sms_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'work_mt_sms_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'work_mo_duration_secs', 'type': 'INTEGER','mode': 'NULLABLE'},
            {'name': 'work_mt_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_data_cdr_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_data_in_volume_kb', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'cnt_of_distinct_bnumber', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'home_data_session_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'home_data_out_volume_kb', 'type': 'INTEGER','mode': 'NULLABLE'},
            {'name': 'work_data_session_count', 'type': 'INTEGER','mode': 'NULLABLE'},
            {'name': 'work_data_out_volume_kb', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mo_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mt_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mo_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mt_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mo_ce_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mt_ce_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mo_ce_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mt_ce_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_abnormal_session_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_abnormal_session_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_data_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_data_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'mo_handoff_cell_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'mt_handoff_cell_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'mo_handoff_technology_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'mt_handoff_technology_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'mo_handoff_mnc_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'mt_handoff_mnc_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_csd_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_csd_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mt_dropcalls_ran_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mt_dropcalls_core_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mt_dropcalls_ran_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mt_dropcalls_core_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mo_dropcalls_ran_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'home_mo_dropcalls_core_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mo_dropcalls_ran_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'work_mo_dropcalls_core_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'process_run_sequence', 'type': 'INTEGER', 'mode': 'NULLABLE' }
        ],
        skip_leading_rows=1,
        source_format='CSV',
        compression='GZIP',
        field_delimiter='|',
        create_disposition='CREATE_NEVER',
        write_disposition='WRITE_TRUNCATE',
        #autodetect=True,
        #encoding='UTF-8',
    )

    load_job = BigQueryExecuteQueryOperator(
        task_id="staging_to_final_table",
        sql=t_agg_dly_load_sql_statement,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )
    
    insert_timestamp = BigQueryExecuteQueryOperator(
        task_id="insert_timestamp",
        sql=t_agg_dly_timestamp_sql_statement,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )

#     backup_raw_file = GCSToGCSOperator(
#         task_id='backup_raw_file',
#         source_bucket='bt-raw-ccn-net-mobnet-edwh-agg-dly-subscriber-usage',
#   #      source_objects=['EE_T_AGG_dly_SUBSCRIBER_USAGE_Milestone_002*.gz','EE_T_AGG_dly*.dat'],
#         source_objects=['EE_T_AGG_dly*.gz'],
#         destination_bucket='bt-raw-ccn-net-mobnet-edwh-agg-dly-subscriber-usage',
#         # destination_object='backup' + "/" + datetime.datetime.now().strftime("%Y%m%d%H") + '*.gz',    
#         destination_object='backup' + "/" + 'EE_T_AGG_dly',
#         move_object=True
#     )
    backup_raw_file = GCSToGCSOperator(
        task_id='backup_raw_file',
        source_bucket=raw_bucket_name,
        source_objects=[input_file],
        destination_bucket=raw_bucket_name,
        destination_object='backup' + "/" + 'BKP_EE_T_AGG_DLY',
        move_object=True
        
    )
    
#     backup_clean_file = GCSToGCSOperator(
#         task_id='backup_clean_file',
#         #source_project='bt-net-mobnet-r-dev-data',
#         source_bucket='bt-cleansed-ccn-net-mobnet-edwh-agg-dly-subs-usage',
#         source_objects=['subs-usage-clean-00000-of-00001.csv'],
#         #destination_project='bt-net-mobnet-r-dev-data',
#         destination_bucket='bt-cleansed-ccn-net-mobnet-edwh-agg-dly-subs-usage',
#         destination_object='backup' + "/" + datetime.datetime.now().strftime("%Y%m%d%H%M")+'subs-usage-clean.csv',
#         move_object=True
#     )  
        
    list_files >> save_file_count >> start_gcs_to_bq_job >> load_job >> insert_timestamp >> backup_raw_file 
