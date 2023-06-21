import datetime
from datetime import timedelta
import os
from airflow import models
from airflow.models import BaseOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators.bigquery_operator import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from airflow.models import Variable

dag_config = Variable.get('dim_cell_dag_variables', deserialize_json=True, default_var={})
dim_cell_merge_sql_statement = Variable.get('dim_cell_merge_sql', default_var={})

bucket_path = dag_config.get("bucket_path")
raw_bucket_name = dag_config.get("raw_bucket_name")
input_file = dag_config.get("input_file")
output_file_name = dag_config.get("output_file_name")
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
output_file_header = dag_config.get('output_file_header')
input_file_headers = dag_config.get('input_file_headers')
primary_key = dag_config.get('primary_key')
columns_to_remove = dag_config.get('columns_to_remove')
dim_cell_destination_dataset = dag_config.get('dim_cell_destination_dataset')

GCP_DEFAULT_CONN = 'google_cloud_default'

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
        dag_id='dim_cell_file_composer_dag',
        schedule_interval=schedule_interval,
        max_active_runs=1,
        catchup=False,
        tags=['3gss'],
        default_args=default_args) as dag:

    # start_template_job = DataflowTemplateOperator(
    #     # The task id of your job
    #     task_id="dataflow_operator_dim_cell_batch",
    #     template=df_template,
    #     # Use the link above to specify the correct parameters for your template.
    #     parameters={
    #         "input": bucket_path + "/" + input_file,
    #         "output": output,
    #         "input_file_headers": input_file_headers,
    #         "output_file_header": output_file_header,
    #         "primary_key": primary_key,
    #         "columns_to_remove": columns_to_remove,
    #         "delimeter": "|"
    #     },
    #     gcp_conn_id=GCP_DEFAULT_CONN,
    #     job_name=df_job_name + '-' + datetime.datetime.now().strftime("%Y%m%d%H")
    # )
    
    backup_raw_file = GCSToGCSOperator(
        task_id='backup_raw_file',
        source_bucket=raw_bucket_name,
        source_objects=[input_file],
        destination_bucket=raw_bucket_name,
        destination_object='backup' + "/" + 'BKP_EE_DIM_CELL',
        move_object=True
    )

    start_gcs_to_bq_job = GCSToBigQueryOperator(
        task_id='gcs_to_bq_dim_cell',
        bucket=raw_bucket_name,
        source_objects=[input_file],
        destination_project_dataset_table=dim_cell_destination_dataset,
        schema_fields=[
            {'name': 'cell_data_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'effective_from_date_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'effective_to_date_id', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'ims_date', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'node_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'node_name', 'type': 'STRING','mode': 'NULLABLE'},
            {'name': 'node_class', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'configuration_data_1', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'configuration_data_2', 'type': 'INTEGER', 'mode': 'NULLABLE' },
            {'name': 'configuration_data_3', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'site_code', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'site_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'bst_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'engineering_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'parent_node', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'antenna_bearing', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'cell_generation', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'site_address_line_1', 'type': 'STRING','mode': 'NULLABLE'},
            {'name': 'site_address_line_2', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'site_address_line_3', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'site_address_line_4', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'county', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'site_number', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'site_area_description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'acquisition_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'radius', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'begin_angle', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'end_angle', 'type': 'NUMERIC', 'mode': 'NULLABLE' },
            {'name': 'cellsector_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'node_county', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'node_site_number', 'type': 'STRING','mode': 'NULLABLE'},
            {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'cell_id', 'type': 'NUMERIC', 'mode': 'NULLABLE'},
            {'name': 'lac', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'site_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'max_cell_radius', 'type': 'NUMERIC', 'mode': 'NULLABLE' },
            {'name': 'mnc', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cell_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'latitude', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'longitude', 'type': 'STRING','mode': 'NULLABLE'},
            {'name': 'northing', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'easting', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'antenna_type', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'post_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'mcc', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'loaded_dt', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'bsc', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'altitude', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cell', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'accmin', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'antenna_gain', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'bcc', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'bcchno', 'type': 'STRING','mode': 'NULLABLE'},
            {'name': 'bspwr', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'bspwrb', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'c_sys_type', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'cell_dir', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'cell_type', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'env_char', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'height', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'max_altitude', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'min_altitude', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ncc', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'sector_angle', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'talim', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'antenna_tilt', 'type': 'STRING','mode': 'NULLABLE'},
            {'name': 'override_ta', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'node_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'enodebid', 'type': 'NUMERIC', 'mode': 'NULLABLE' },
            {'name': 'global_cell_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'technology', 'type': 'STRING', 'mode': 'NULLABLE' },
            {'name': 'process_run_sequence', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'sourced_from', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],

        skip_leading_rows=1,
        source_format='CSV',
        field_delimiter='|',
        compression='GZIP',
        create_disposition='CREATE_NEVER',
        write_disposition='WRITE_TRUNCATE',
        #autodetect=True,
        #encoding='UTF-8',
    )
    
    merge_table = BigQueryExecuteQueryOperator(
        task_id="merge_table",
        sql=dim_cell_merge_sql_statement,
        write_disposition="WRITE_APPEND",
        gcp_conn_id=GCP_DEFAULT_CONN,
        use_legacy_sql=False,
    )
    
    # backup_clean_file = GCSToGCSOperator(
    #     task_id='backup_clean_file',
    #     #source_project='bt-net-mobnet-r-dev-data',
    #     source_bucket=clean_output_bucket,
    #     source_objects=[output_file_name +'*.csv'],
    #     #destination_project='bt-net-mobnet-r-dev-data',
    #     destination_bucket=clean_output_bucket,
    #     destination_object='backup' + "/" + datetime.datetime.now().strftime("%Y%m%d%H%M") + "/" + output_file_name + '.csv',
    #     move_object=True
    #Hello )     


   
    start_gcs_to_bq_job >> backup_raw_file >> merge_table
    # start_template_job >> backup_raw_file >> start_gcs_to_bq_job >> merge_table >> backup_clean_file

