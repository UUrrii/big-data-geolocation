import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from pyspark.sql.functions import desc


def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(description='Cleaning GeoLocation Data.')
    parser.add_argument("--year", help="Partition Year To Process", required=True, type=str)
    parser.add_argument("--month", help="Partition Month To Process", required=True, type=str)
    parser.add_argument("--day", help="Partition Day To Process", required=True, type=str)
    parser.add_argument('--hdfs_source_dir', help='HDFS source directory', required=True, type=str)
    parser.add_argument('--hdfs_target_dir', help='HDFS target directory', required=True, type=str)
    parser.add_argument('--hdfs_target_format', help='HDFS target format', required=True, type=str)
    return parser.parse_args()


if __name__ == '__main__':
    """
    Main Function
    """
    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read ipv4 from HDFS
    ipv4_dataframe = spark.read.format(args.hdfs_target_format)\
        .options(header='true', delimiter=',', nullValue='null', inferschema='true')\
        .load(f'{args.hdfs_source_dir}/ipv4/{args.year}/{args.month}/{args.day}/*.{args.hdfs_target_format}')

    # Read data from HDFS
    data_dataframe = spark.read.format(args.hdfs_target_format)\
        .options(header='true', delimiter=',', nullValue='null', inferschema='true')\
        .load(f'{args.hdfs_source_dir}/data/{args.year}/{args.month}/{args.day}/*.{args.hdfs_target_format}')

    # Clean Dataframes
    ipv4_dataframe = ipv4_dataframe.select('network', 'geoname_id', 'latitude', 'longitude')
    ipv4_dataframe.show()

    data_dataframe = data_dataframe.select('geoname_id', 'continent_name', 'country_name', 'subdivision_1_name', 'city_name')
    data_dataframe.show()

    # Save clean Dataframes
    ipv4_dataframe.write.format(args.hdfs_target_format).mode('overwrite').save(f'{args.hdfs_target_dir}/ipv4/{args.year}/{args.month}/{args.day}')
    data_dataframe.write.format(args.hdfs_target_format).mode('overwrite').save(f'{args.hdfs_target_dir}/data/{args.year}/{args.month}/{args.day}')
