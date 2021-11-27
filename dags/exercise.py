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
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator

args = {
    'owner': 'airflow'
}

license_key = 'ioal7iIr53N4clk2'
download_url = 'https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City-CSV&license_key=' + license_key + '&suffix=zip'

# MySql and Hive Scripts
sql_create_ipv4_table = '''
    CREATE TABLE IF NOT EXISTS ipv4 (
    network STRING,
    geoname_id BIGINT,
    latitude FLOAT,
    longitude FLOAT) COMMENT 'IPV4' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/geolocation/final/ipv4/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/*.csv';
'''

sql_create_data_table = '''
    CREATE TABLE IF NOT EXISTS data (
    geoname_id BIGINT,
    continent_name STRING,
    country_name STRING,
    subdivision_1_name STRING,
    city_name STRING) COMMENT 'Data' PARTITIONED BY (partition_year int, partition_month int, partition_day int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/hadoop/geolocation/final/data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/*.csv';
'''

sql_add_partition_ipv4='''
    ALTER TABLE ipv4
    ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
    LOCATION '/user/hadoop/geolocation/final/ipv4/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/*.csv';
'''

sql_add_partition_data='''
    ALTER TABLE data
    ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
    LOCATION '/user/hadoop/geolocation/final/data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/*.csv';
'''

mysql_create_data_table = '''
CREATE TABLE IF NOT EXISTS data(
    geoname_id BIGINT,
    continent_name TEXT,
    country_name TEXT,
    subdivision_1_name TEXT,
    city_name TEXT
);
'''

mysql_create_ipv4_table = '''
CREATE TABLE IF NOT EXISTS ipv4(
    network TEXT,
    geoname_id BIGINT,
    latitude FLOAT,
    longitude FLOAT
);
'''

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
    local_file='/home/airflow/geolocation/GeoLite2-City-CSV_20211123/GeoLite2-City-Blocks-IPv4.csv',
    remote_file='/user/hadoop/geolocation/raw/ipv4/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/ipv4.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

hdfs_put_data = HdfsPutFileOperator(
    task_id='put_data_to_hdfs',
    local_file='/home/airflow/geolocation/GeoLite2-City-CSV_20211123/GeoLite2-City-Locations-de.csv',
    remote_file='/user/hadoop/geolocation/raw/data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/data.csv',
    hdfs_conn_id='hdfs',
    dag=dag,
)

dummy_op = DummyOperator(
    task_id='dummy',
    dag=dag
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

# Hive Tasks
create_table_ipv4 = HiveOperator(
    task_id='create_table_ipv4',
    hql=sql_create_ipv4_table,
    hive_cli_conn_id='beeline',
    dag=dag
)

create_table_data = HiveOperator(
    task_id='create_table_data',
    hql=sql_create_data_table,
    hive_cli_conn_id='beeline',
    dag=dag
)

add_partition_ipv4 = HiveOperator(
    task_id='add_partition_ipv4',
    hql=sql_add_partition_ipv4,
    hive_cli_conn_id='beeline',
    dag=dag,
)

add_partition_data = HiveOperator(
    task_id='add_partition_data',
    hql=sql_add_partition_data,
    hive_cli_conn_id='beeline',
    dag=dag,
)

# Create Connection
mysql_connection = '''
    airflow connections -a \
        --conn_id 'mysql_connection_geolocation' \
        --conn_type 'mysql' \
        --conn_login 'root' \
        --conn_password 'root' \
        --conn_host 'localhost' \
        --conn_port '3306' \
        --conn_schema 'geolocation' \
        --conn_extra '{"charset": "utf8"}'
'''

create_mysql_connection = BashOperator(
    task_id='create_mysql_connection',
    bash_command=mysql_connection,
    dag=dag,
)

delete_mysql_connection = BashOperator(
    task_id='delete_mysql_connection',
    bash_command='airflow connections -d --conn_id mysql_connection_geolocation',
    dag=dag,
)

# MySql Tasks
create_mysql_table_ipv4 = MySqlOperator(
    task_id='create_mysql_table_ipv4',
    sql=mysql_create_ipv4_table,
    mysql_conn_id='mysql_connection_geolocation',
    database='geolocation',
    dag=dag,
)

create_mysql_table_data = MySqlOperator(
    task_id='create_mysql_table_data',
    sql=mysql_create_data_table,
    mysql_conn_id='mysql_connection_geolocation',
    database='geolocation',
    dag=dag,
)

# Hive to MySql
hive_ipv4_to_mysql = HiveToMySqlTransfer(
    task_id='hive_ipv4_to_mysql',
    sql="SELECT * FROM ipv4",
    hive_cli_conn_id='beeline',
    mysql_table='ipv4',
    mysql_conn_id='mysql_connection_geolocation',
    dag=dag,
)

hive_data_to_mysql = HiveToMySqlTransfer(
    task_id='hive_data_to_mysql',
    sql="SELECT * FROM data",
    hive_cli_conn_id='beeline',
    mysql_table='data',
    mysql_conn_id='mysql_connection_geolocation',
    dag=dag,
)


# Ablauf
create_local_import_dir >> clear_local_import_dir
clear_local_import_dir >> download_geolocation_city_data
download_geolocation_city_data >> unzip_geolocation_city_data
unzip_geolocation_city_data >> create_hdfs_geolocation_ipv4_dir
unzip_geolocation_city_data >> create_hdfs_geolocation_data_dir
create_hdfs_geolocation_ipv4_dir >> hdfs_put_ipv4
create_hdfs_geolocation_data_dir >> hdfs_put_data
hdfs_put_ipv4 >> create_table_ipv4
hdfs_put_data >> create_table_data
create_table_ipv4 >> dummy_op
create_table_data >> dummy_op
dummy_op >> pyspark_clean_data
pyspark_clean_data >> add_partition_ipv4
pyspark_clean_data >> add_partition_data
add_partition_ipv4 >> delete_mysql_connection
add_partition_data >> delete_mysql_connection
delete_mysql_connection >> create_mysql_connection
create_mysql_connection >> create_mysql_table_data
create_mysql_table_data >> create_mysql_table_ipv4
create_mysql_table_ipv4 >> hive_data_to_mysql
create_mysql_table_ipv4 >> hive_ipv4_to_mysql
