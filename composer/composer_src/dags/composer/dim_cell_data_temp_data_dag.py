from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 1),
}

with DAG('load_table_dag', default_args=default_args, schedule_interval=None) as dag:
    
    load_table_task = BigQueryOperator(
        task_id='load_table',
        sql='SELECT cell_data_id, effective_from_date_id, effective_to_date_id, ims_date, node_id, node_name, node_class, configuration_data_1, configuration_data_2, configuration_data_3, site_code, site_name, bst_status, engineering_name, region, parent_node, antenna_bearing, cell_generation, site_address_line_1, site_address_line_2, site_address_line_3, site_address_line_4, county, site_number, site_area_description, acquisition_status, radius, begin_angle, end_angle, cellsector_name, node_county, node_site_number, source, cell_id, lac, site_id, max_cell_radius, mnc, cell_status, latitude, longitude, northing, easting, antenna_type, post_code, mcc, loaded_dt, bsc, altitude, cell, accmin, antenna_gain, bcc, bcchno, bspwr, bspwrb, c_sys_type, cell_dir, cell_type, env_char, height, max_altitude, min_altitude, ncc, sector_angle, talim, antenna_tilt, override_ta, node_status, enodebid, global_cell_id, technology, process_run_sequence, sourced_from, record_ts FROM `bt-tch-3g-sunset-dp-dev.net_mobnet_rw.dim_cell_data_staging`',
        destination_dataset_table='bt-tch-3g-sunset-dp-dev.net_mobnet_rw.dim_cell_data_temp_data',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
    )

    load_table_task
