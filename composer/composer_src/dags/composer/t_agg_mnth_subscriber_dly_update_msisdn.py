import datetime
from datetime import timedelta

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

dag_config = Variable.get('t_agg_mnth_subscriber_dly_update_msisdn_dag_variables', deserialize_json=True, default_var={})

t_agg_dly_msisdn_update_sql_statement = Variable.get('t_agg_dly_msisdn_update_sql', default_var={})
t_agg_mnth_msisdn_update_sql_statement = Variable.get('t_agg_mnth_msisdn_update_sql', default_var={})


project_id = dag_config.get("project_id")
bq_project_id = dag_config.get("bq_project_id")
bq_output_dataset = dag_config.get("bq_output_dataset")
bq_output_table = dag_config.get("bq_output_table")
gce_zone = dag_config.get("gce_zone")
output = dag_config.get("output")
gce_region = dag_config.get("gce_region")
schedule_interval = dag_config.get('schedule_interval')




primary_key = dag_config.get('primary_key')
columns_to_remove = dag_config.get('columns_to_remove')


GCP_DEFAULT_CONN = 'google_cloud_default'

default_args = {
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2021, 5, 19),
    "location": gce_region,

}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        dag_id='t_agg_mnth_subscriber_dly_update_msisdn_dag',
        schedule_interval=schedule_interval,
        max_active_runs=1,
        catchup=False,
        tags=['3gss'],
        default_args=default_args) as dag:
    

   #start_gcs_to_bq_job = GCSToBigQueryOperator(
#  #    task_id='gcs_to_bq_agg_mth_temp',
#   #   bucket=raw_bucket_name,
#    #  source_objects=[input_file],
#       destination_project_dataset_table=agg_mth_destination_dataset,
#      schema_fields=[
#          {'name': 'month_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'msisdn', 'type': 'STRING', 'mode': 'NULLABLE'},
#          {'name': 'exbr_account_num', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'css_account_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'jt_customer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},            
#          {'name': 'subscription_sk', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'imei', 'type': 'STRING', 'mode': 'NULLABLE'},
#          {'name': 'imsi', 'type': 'STRING', 'mode': 'NULLABLE'},
#          {'name': 'first_cell_data_id', 'type': 'INTEGER','mode': 'NULLABLE'},
#          {'name': 'last_cell_data_id', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mo_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'home_mt_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'home_mo_sms_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'home_mt_sms_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mo_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'home_mt_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'home_data_cdr_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'home_data_in_volume_kb', 'type': 'INTEGER','mode': 'NULLABLE'},
#          {'name': 'work_mo_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mt_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'work_mo_sms_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'work_mt_sms_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'work_mo_duration_secs', 'type': 'INTEGER','mode': 'NULLABLE'},
#          {'name': 'work_mt_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_data_cdr_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_data_in_volume_kb', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'cnt_of_distinct_bnumber', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'home_data_session_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
#          {'name': 'home_data_out_volume_kb', 'type': 'INTEGER','mode': 'NULLABLE'},
#          {'name': 'work_data_session_count', 'type': 'INTEGER','mode': 'NULLABLE'},
#          {'name': 'work_data_out_volume_kb', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mo_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mt_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mo_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mt_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mo_ce_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mt_ce_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mo_ce_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mt_ce_dropcalls_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_abnormal_session_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_abnormal_session_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_data_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_data_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'mo_handoff_cell_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'mt_handoff_cell_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'mo_handoff_technology_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'mt_handoff_technology_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'mo_handoff_mnc_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'mt_handoff_mnc_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_csd_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_csd_duration_secs', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mt_dropcalls_ran_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mt_dropcalls_core_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mt_dropcalls_ran_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mt_dropcalls_core_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mo_dropcalls_ran_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'home_mo_dropcalls_core_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mo_dropcalls_ran_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'work_mo_dropcalls_core_count', 'type': 'INTEGER', 'mode': 'NULLABLE' },
#          {'name': 'process_run_sequence', 'type': 'INTEGER', 'mode': 'NULLABLE' }
#      ],
#      skip_leading_rows=1,
#      source_format='CSV',
#      compression='GZIP',
#      field_delimiter=',',
#      create_disposition='CREATE_NEVER',
#      write_disposition='WRITE_TRUNCATE',
#      #autodetect=True,
#      #encoding='UTF-8',
#  )


    
    update_msisdn_dly= BigQueryExecuteQueryOperator(
        task_id="update_msisdn_field_dly",
        sql=t_agg_dly_msisdn_update_sql_statement,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )

    update_msisdn_mnth= BigQueryExecuteQueryOperator(
        task_id="update_msisdn_field_mnth",
        sql=t_agg_mnth_msisdn_update_sql_statement,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )
#     backup_raw_file = GCSToGCSOperator(
#         task_id='backup_raw_file',
#         source_bucket='bt-raw-ccn-net-mobnet-edwh-agg-mth-subscriber-usage',
#   #      source_objects=['EE_T_AGG_MTH_SUBSCRIBER_USAGE_Milestone_002*.gz','EE_T_AGG_MTH*.dat'],
#         source_objects=['EE_T_AGG_MTH*.gz'],
#         destination_bucket='bt-raw-ccn-net-mobnet-edwh-agg-mth-subscriber-usage',
#         # destination_object='backup' + "/" + datetime.datetime.now().strftime("%Y%m%d%H") + '*.gz',    
#         destination_object='backup' + "/" + 'EE_T_AGG_MTH',
#         move_object=True
#  
    
#     backup_clean_file = GCSToGCSOperator(
#         task_id='backup_clean_file',
#         #source_project='bt-net-mobnet-r-dev-data',
#         source_bucket='bt-cleansed-ccn-net-mobnet-edwh-agg-mth-subs-usage',
#         source_objects=['subs-usage-clean-00000-of-00001.csv'],
#         #destination_project='bt-net-mobnet-r-dev-data',
#         destination_bucket='bt-cleansed-ccn-net-mobnet-edwh-agg-mth-subs-usage',
#         destination_object='backup' + "/" + datetime.datetime.now().strftime("%Y%m%d%H%M")+'subs-usage-clean.csv',
#         move_object=True
#     )  
        
    update_msisdn_dly >> update_msisdn_mnth
