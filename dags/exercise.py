from datetime import datetime
from airflow import DAG
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


args = {
    'owner': 'airflow'
}

license_key = 'ioal7iIr53N4clk2'
download_url = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City-CSV&license_key=' + license_key + '&suffix=zip'

# Dag Start
dag = DAG('big-data-exercise', default_args=args, description='Create Searchable IP and Geolocation Database',
          schedule_interval='56 18 * * *',
          start_date=datetime(2021, 11, 15), catchup=False, max_active_runs=1)

# Download Data
create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='geolocation',
    dag=dag,
)

clear_local_import_dir = BashOperator(
    task_id='clear_import_dir',
    bash_command='rm /home/airflow/geolocation/* -rf',
    dag=dag
)

download_geolocation_city_data = HttpDownloadOperator(
    task_id='download_geolocation_city_data',
    download_uri=download_url,
    save_to='/home/airflow/geolocation/geolocation_city.zip',
    dag=dag,
)

unzip_geolocation_city_data = BashOperator(
    task_id='unzip_geolocation_city_data',
    bash_command='unzip /home/airflow/geolocation/geolocation_city -d /home/airflow/geolocation',
    dag=dag
)

# HDFS Tasks
create_hdfs_geolocation_ipv4_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_geolocation_ipv4_dir',
    directory='/user/hadoop/geolocation/raw/ipv4/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag
)

create_hdfs_geolocation_data_dir = HdfsMkdirFileOperator(
    task_id='create_hdfs_geolocation_data_dir',
    directory='/user/hadoop/geolocation/raw/data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
    hdfs_conn_id='hdfs',
    dag=dag
)

hdfs_put_ipv4 = HdfsPutFileOperator(
    task_id='put_ipv4_to_hdfs',
    local_file='/home/airflow/geolocation/GeoLite2-City-CSV_20211130/GeoLite2-City-Blocks-IPv4.csv',
    remote_file='/user/hadoop/geolocation/raw/ipv4/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/ipv4.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_data = HdfsPutFileOperator(
    task_id='put_data_to_hdfs',
    local_file='/home/airflow/geolocation/GeoLite2-City-CSV_20211130/GeoLite2-City-Locations-de.csv',
    remote_file='/user/hadoop/geolocation/raw/data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/data.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

# Clean Data
pyspark_clean_data = SparkSubmitOperator(
    task_id='pyspark_clean_data',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_clean_data.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_clean_data',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/geolocation/raw', '--hdfs_target_dir', '/user/hadoop/geolocation/final', '--hdfs_target_format', 'csv'],
    dag=dag
)

pyspark_clean_ipv4 = SparkSubmitOperator(
    task_id='pyspark_clean_ipv4',
    conn_id='spark',
    application='/home/airflow/airflow/python/pyspark_clean_ipv4.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='spark_clean_ipv4',
    verbose=True,
    application_args=['--year', '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}', '--month', '{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}', '--day',  '{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}', '--hdfs_source_dir', '/user/hadoop/geolocation/raw', '--hdfs_target_dir', '/user/hadoop/geolocation/final', '--hdfs_target_format', 'csv'],
    dag=dag
)


# Ablauf
create_local_import_dir >> clear_local_import_dir
clear_local_import_dir >> download_geolocation_city_data
download_geolocation_city_data >> unzip_geolocation_city_data
unzip_geolocation_city_data >> create_hdfs_geolocation_ipv4_dir
unzip_geolocation_city_data >> create_hdfs_geolocation_data_dir
create_hdfs_geolocation_ipv4_dir >> hdfs_put_ipv4
create_hdfs_geolocation_data_dir >> hdfs_put_data
hdfs_put_ipv4 >> pyspark_clean_ipv4
hdfs_put_data >> pyspark_clean_data
