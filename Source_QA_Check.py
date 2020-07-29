import sys
import pyspark.sql
from pyspark import storagelevel
from SparkBuilder import get_spark_session
from SparkBuilder import get_source_header, file_info
from pyspark.sql.functions import col, max


def header_reader_from_source(sc_con, file_path):
    sc = spark_con.sparkContext
    rwdata = sc.textFile(file_path)
    header = rwdata.first()
    return header


def read_sourcefile(spark_con, file_path):
    source_raw_data = spark_con.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
#    header_row = source_raw_data.he
    return source_raw_data


def read_header_from_config(source_file_name):
     head_reader = get_source_header(source_file_name)
     return head_reader


def is_header_match(src_header, config_header):
    if src_header == config_header:
        match_result = "matching"
    else:
        match_result = "notmatching"
    return match_result


def write_to_csv(df, file_path):
    df.repartition(1).write.csv(file_path)
    #   It will create multiple csv file spark 2.0+ version
    #   Max_Credit.write.csv("sample_data/Max_Credit.csv")
    #   It will create single csv file spark 2.0+ version
    #   Max_Credit.coalesce(1).write.csv("sample_data/Max_Credit_Single.csv")
    return file_path+" file created successfully"


def filter_not_null_key_filed(key_field, df):
    not_null_key_data = df.where(col(key_field).isNotNull())
    return not_null_key_data


def filter_null_key_filed(key_field, df):
    null_key_data = df.where(col(key_field).isNull())
    return null_key_data


def convert_to_valid_column_order(df,spark_con, header, Temp_View_Name):
    df.createOrReplaceTempView(Temp_View_Name)
    query = "Select " + header + " from " + Temp_View_Name
    valid_column_order = spark_con.sql(query)
    return valid_column_order


def get_only_cleaned_data(valid_df, keyfield, orderbyfield, columndetails, TempViewTblName, filepathtostore):
    regex = r"[a-zA-z\s/#$%^*&():;'.@!`~]{1,}"
    data_after_junk_rm = valid_df.filter(col(keyfield).rlike(regex) == False)
    data_after_junk_rm.createOrReplaceTempView(TempViewTblName)
    query = "SELECT " + columndetails + " FROM ( SELECT " + columndetails + ",ROW_NUMBER() OVER (PARTITION BY " + keyfield + " ORDER BY " + orderbyfield + " desc) AS RowNumber FROM " + TempViewTblName + " where "+keyfield+" is not null ) AS d WHERE RowNumber= 1"
    final_data = spark_con.sql(query)
    return final_data


def get_only_non_valid_data(valid_df, keyfield, orderbyfield, columndetails, TempViewTblName, filepathtostore):
    regex = r"[a-zA-z\s/#$%^*&():;'.@!`~]{1,}"
    data_with_junk = valid_df.filter(col(keyfield).rlike(regex))
    valid_df.createOrReplaceTempView(TempViewTblName)
    query = "SELECT " + columndetails + " FROM ( SELECT " + columndetails + ",ROW_NUMBER() OVER (PARTITION BY " + keyfield + " ORDER BY " + orderbyfield + " desc) AS RowNumber FROM " + TempViewTblName + " where "+keyfield+" is not null ) AS d WHERE RowNumber!= 1"
    duplicate_data = spark_con.sql(query)
    Nulldata = filter_null_key_filed(keyfield, valid_df)
    return data_with_junk, duplicate_data, Nulldata


def workflow(valid_df, keyfield, orderbyfield, columndetails, TempViewTblName, filewithpathname,opfilepath):
    finaldata = get_only_cleaned_data(valid_df, keyfield, orderbyfield, columndetails, TempViewTblName,
                                      filewithpathname)
    print("step 8 : Null values , duplicate records , junk key values are filtered ")
    data_with_junk, duplicate_data, Nulldata = get_only_non_valid_data(valid_df, keyfield, orderbyfield, columndetails,
                                                                       TempViewTblName, filewithpathname)
    print("step 8.1 Junk/non_valid key ")
    data_with_junk.show()
    print("step 8.2 key with null value ")
    Nulldata.show()
    print("step 8.3 key with duplicate value ")
    duplicate_data.show()
    print("step 9 : finding Max ( credit ) from Customer_credit_rate.csv")
    Max_Credit = finaldata.groupBy(col(keyfield)).agg({'credit': 'max'}).alias('max_credit').orderBy(col(keyfield))
    print("step 10 : final aggregated results are stored into Max_Credit_Single_Partition.csv")
    Max_Credit.show()
    process_result = write_to_csv(Max_Credit, opfilepath + "/Max_Credit_Single_Partition.csv")
    print(process_result)


# Getting config details
FileDic = file_info("SrcFileDetails")
keyfield = FileDic["file.keyfield"]
orderbyfield =FileDic["file.orderbyfield"]
columndetails =FileDic["file.columndetails"]
TempViewTblName =FileDic["file.tempviewtblname"]
filewithpathname =FileDic["file.filewithpathname"]
opfilepath =FileDic["file.opfilepath"]


print("Step 1 : Get Spark Session")
spark_con = get_spark_session()
print("Step 2 : Read source header from config file ")
header_list = read_header_from_config("CreditRate")
print("Step 3 : The source header from config file = {} ".format(header_list[0]))
head = header_reader_from_source(spark_con, filewithpathname)
print("Step 4 : The header from source file {} ".format(head))
header = header_list[0]
header_match_result = is_header_match(head, header)
print("Step 5 : Is the source header  matching/notmatching  ? = {} ".format(header_match_result))
raw_df = read_sourcefile(spark_con, "sample_data/Customer_credit_rate.csv")
print("Step 6 : Read data from source file completed , this data will be shared for data clean ")

if header_match_result == "matching":
    print("step 7 : source file header matching loop started")
    finaldata = get_only_cleaned_data(raw_df, keyfield, orderbyfield, columndetails, TempViewTblName, filewithpathname)
    workflow(finaldata, keyfield, orderbyfield, columndetails, TempViewTblName, filewithpathname, opfilepath)

#   Max_Credit = raw_df.groupBy(col('Cust_id')).agg(max(col('credit'))).alias('max_credit').orderBy(col('Cust_id'))
#   Max_Credit = raw_df.groupBy(col('Cust_id')).agg({'credit':'max'}).alias('max_credit').orderBy(col('Cust_id'))
#   process_result = write_to_csv(Max_Credit, "sample_data/Max_Credit_Single_Partition.csv")
#   print(process_result)

else:
    print("step 7 : source file header notmatching loop started")
    valid_df = convert_to_valid_column_order(raw_df, spark_con, header, "Customer_credit_rate")
    workflow(valid_df, keyfield, orderbyfield, columndetails, TempViewTblName, filewithpathname, opfilepath)

spark_con.stop()


