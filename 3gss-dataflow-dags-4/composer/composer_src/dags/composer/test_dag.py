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

# def save_value(ti) -> None:
#     dt = ti.xcom_pull(task_ids=['list_files'])
#     if not dt:
#         raise ValueError('No value currently stored in XComs.')

#     print('The value of xcom is',dt)
#     if int(dt[0]) >= 1: 
#         print('The next dag can be triggered')
#     else:
#         print('Files have not arrived yet')    
#     # with open('/Users/dradecic/airflow/data/date.txt', 'w') as f:
#     #     f.write(dt[0])


# def _is_ok(ti) -> None:
#     dt = ti.xcom_pull(task_ids=['list_files'])
#     if int(dt[0]) >= 1:
#         return True
#         return False


dag_config = Variable.get('test_dag_variables', deserialize_json=True, default_var={})
test_load_sql_statement = Variable.get('test_load_sql', default_var={})

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
        dag_id='test_dag',
        schedule_interval=schedule_interval,
        max_active_runs=1,
        catchup=False,
        tags=['3gss'],
        default_args=default_args) as dag:

    # Clear_table = BigQueryExecuteQueryOperator(
    #     task_id="Clear_table",
    #     sql=""" delete from `bt-tch-3g-sunset-dp-prod.dp_consumer_rw.usage_agg_mth` where 1=1 """,
    #     write_disposition="WRITE_APPEND",
    #     gcp_conn_id=GCP_DEFAULT_CONN,
    #     use_legacy_sql=False,
    # )

    # list_files = bash.BashOperator(
    #     task_id="list_files",
    #     # Executing 'bq' command requires Google Cloud SDK which comes
    #     # preinstalled in Cloud Composer.
    #     #bash_command=f"if[ gsutil ls gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l ] -ge 10; ",
    #     bash_command=f"gsutil du gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE*.dat.gz | wc -l",
    #     do_xcom_push=True,
    #     #bash_command=f"echo $files",

    #     # gsutil ls gs://bucket/folder/** | wc -l
    # )

    # task_save_value = PythonOperator(
    #     task_id='save_value',
    #     python_callable=save_value
    # )

    # _is_ok = ShortCircuitOperator(
    #     task_id='_is_ok',
    #     python_callable=_is_ok,
    # )

#     checkfile = bash.BashOperator(
#         task_id="checkfile",
#         # Executing 'bq' command requires Google Cloud SDK which comes
#         # preinstalled in Cloud Composer.
#         #bash_command=f"if[ gsutil ls gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l ] -ge 10; ",
#         #bash_command=f"files=(gsutil du gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l)",
#         bash_command="cd ..; cat new.txt ",
#         do_xcom_push=False,
#         # gsutil ls gs://bucket/folder/** | wc -l
#     )
#     # checkfile = bash.BashOperator(
#     #     task_id="checkfile",
#     #     # Executing 'bq' command requires Google Cloud SDK which comes
#     #     # preinstalled in Cloud Composer.
#     #     #bash_command=f"if[ gsutil ls gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l ] -ge 10; ",
#     #     #bash_command=f"files=(gsutil du gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l)",
#     #     bash_command="echo 'XCom fetched: {{ ti.xcom_pull(task_ids=[\'list_files\']) }}'",
#     #     do_xcom_push=False,
#     #     # gsutil ls gs://bucket/folder/** | wc -l
#     # )

#     commands = '''
#                 count=`"gsutil du gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l"`; if [ $count -eq 1 ]; then echo "true"; fi
#                 '''
# #                       count=`ps -ef | grep -c "checkcondition"`; if [ $counter -eq 1 ]; then echo "true"; fi
#     checkcondition = bash.BashOperator(
#         task_id="checkcondition",
#         # Executing 'bq' command requires Google Cloud SDK which comes
#         # preinstalled in Cloud Composer.
#         #bash_command=f"if[ gsutil ls gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l ] -ge 10; ",
#         #bash_command=f"files=(gsutil du gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l)",
#         bash_command=commands,
#         do_xcom_push=False,
#         # gsutil ls gs://bucket/folder/** | wc -l
#     )
    # checkfile = bash.BashOperator(
    #     task_id="checkfile",
    #     # Executing 'bq' command requires Google Cloud SDK which comes
    #     # preinstalled in Cloud Composer.
    #     #bash_command=f"if[ gsutil ls gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l ] -ge 10; ",
    #     #bash_command=f"files=(gsutil du gs://bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage/EE* | wc -l)",
    #     bash_command='pwd',

    #     # gsutil ls gs://bucket/folder/** | wc -l
    # )

    # check_files = GCSObjectExistenceSensor(
    #     task_id='check_files',
    #     bucket='bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage',
    #     object='EE_T_AGG_MTH_SUBSCRIBER_USAGE_20221024_001_016_HASH.dat',
    #     object='EE_T_AGG_MTH_SUBSCRIBER_USAGE_+''20221024_001_016_HASH.dat',
    #     google_cloud_conn_id='google_cloud_default',
    #     timeout=130
    # )
    
    # GCS_Files = GoogleCloudStorageListOperator(
    #     task_id='GCS_Files',
    #     bucket='bt-dev-raw-ccn-mob-nwk-edwh-agg-mth-subscriber-usage',
    #     prefix='EE_T_AGG_MTH_SUBSCRIBER_USAGE_*_HASH',
    #     delimiter='.csv',
    #     gcp_conn_id=GCP_DEFAULT_CONN,
    # )
    
    BQtoBQ_Load_job = BigQueryExecuteQueryOperator(
        task_id="BQtoBQ_Load_job",
        sql=test_load_sql_statement,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )

    # trigger_dependent_dag1 = TriggerDagRunOperator(
    #     task_id="trigger_dependent_dag1",
    #     trigger_dag_id="t_agg_mth_subscriber_usage_dag",
    #     wait_for_completion=True
    # )    
    # trigger_dependent_dag2 = TriggerDagRunOperator(
    #     task_id="trigger_dependent_dag2",
    #     trigger_dag_id="ref_cdm_product_excal_dag",
    #     wait_for_completion=True
    # )

    BQtoBQ_Load_job
    #list_files >> task_save_value >> checkfile >> checkcondition
    #list_files >> task_save_value >> _is_ok >> trigger_dependent_dag1 >> trigger_dependent_dag2
    


