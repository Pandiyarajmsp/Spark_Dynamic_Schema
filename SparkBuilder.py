from pyspark.sql import SparkSession
from pyspark import SparkConf
import configparser
import sys



def get_spark_session():
    spark_config = SparkConf()
    config = configparser.ConfigParser()
    config.read("sample_data/app.properties")

    for config_name, config_value in config.items("CONFIGS"):
        spark_config.set(config_name, config_value)

    # creating Spark Session variable
    try:
        # spark = SparkSession.builder.appName("spark_app_win10").master("local[2]").getOrCreate()
        spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
        return spark
    except Exception as spark_error:
        print(spark_error)
        sys.exit(1)


def get_source_header(src_file_name):
    header_reader = configparser.ConfigParser()
    header_reader.read("sample_data/app.properties")
    Header_Details = []

    for file_name, header_value in header_reader.items(src_file_name):
          Header_Details.append(header_value)

#    return Header_Details
    return Header_Details

def file_info(SrcConfig):
    file_Config = configparser.ConfigParser()
    file_Config.read("sample_data/app.properties")
    file_Details = {}
    for config_name, config_value in file_Config.items(SrcConfig):
        file_Details[config_name] = config_value

    return file_Details



