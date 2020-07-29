from SparkBuilder import get_source_header, file_info
from os.path import join, dirname, abspath
from pyspark.sql.functions import *
from pyspark.sql.types import *
from SparkBuilder import get_spark_session
import pandas as pd
import re


#Getting  DataDictionary column details from Config file

DDColDetails = file_info("DataDicColumns")
FirstColumnName = DDColDetails["firstcolumn"]
SecondDataType = DDColDetails["secondcolumn"]
ThirdIsNullable = DDColDetails["thirdcolumn"]
DataDictFileName = DDColDetails["datadicfilename"]
inputfilepath=DDColDetails["inputfilepath"]

require_cols = [FirstColumnName, SecondDataType , ThirdIsNullable]
#DataDict = pd.read_excel(DataDictFileName, sheet_name=0, na_values="Missing", header=0 , usecols=require_cols)
DataDict = pd.read_excel(DataDictFileName, sheet_name=0, na_values="Missing", header=0)

print("Standard columns are taken from config file ")
print("Data Dict excel should have the following standard columns = {},{},{}".format(FirstColumnName,SecondDataType,ThirdIsNullable))

# create Spark session
spark=get_spark_session()
sc=spark.sparkContext

noofcolumn=len(DataDict.columns)


FirstColRe=r"col"
FirstColName=''
secondColRe=r"[-type-]"
SecondColName=''
ThirdColRe=r"[-nul-]"
ThirdColName=''
Is_first_col_valid=0
Is_Secod_col_valid=0


def DD_Col_Pattern_validation(noofcol, DataDict, FirstColRe, secondColRe, ThirdColRe,FirstColumnName,SecondDataType,ThirdIsNullable):
    Df_DD_Col= {}
    col=DataDict.columns
    Is_first_col_valid = 0
    Is_Secod_col_valid = 0
    FirstColName = ''
    SecondColName = ''
    ThirdColName = ''

    for num in range(0, noofcol):
        print("Data dict having the following column {}".format(col[num]))
        Df_DD_Col[num]=col[num]

    if bool(re.search(FirstColRe,Df_DD_Col.get(0,'none').lower())):
        FirstColName=FirstColumnName
        Is_first_col_valid=1
    if bool(re.search(secondColRe, Df_DD_Col.get(1, 'none').lower())):
        SecondColName = SecondDataType
        Is_Secod_col_valid=1
    if (bool(re.search(ThirdColRe, Df_DD_Col.get(2, 'none').lower())) & noofcol==3):
        ThirdColName = ThirdIsNullable

    return FirstColName, SecondColName, ThirdColName,Is_first_col_valid,Is_Secod_col_valid



def dynnamic_schema_creation(spark,SparkDD_DF,noofcol):
    #  dynamic user defined schema creation from excel input
    if noofcol==3:
        UsDynamic_schema = StructType([
             StructField(name, eval(type), bool(nullable)) for name, type, nullable in SparkDD_DF.rdd.collect()
         ])
        emptyDF = spark.createDataFrame(sc.emptyRDD(), schema=UsDynamic_schema)

    else:
        UsDynamic_schema = StructType([
             StructField(name, eval(type), True) for name, type in SparkDD_DF.rdd.collect()
         ])
        emptyDF = spark.createDataFrame(sc.emptyRDD(), schema=UsDynamic_schema)
    return UsDynamic_schema


def dd_schema_creation(spark,DataDict,FirstColName,SecondColName,ThirdColName,noofcol):
    if noofcol == 3:
        schema = StructType(
            [StructField(FirstColName, StringType(), True), StructField(SecondColName, StringType(), True),
             StructField(ThirdColName, StringType(), True)])
        SparkDD_DF = spark.createDataFrame(DataDict, schema)
    else:
        schema = StructType(
            [StructField(FirstColName, StringType(), True), StructField(SecondColName, StringType(), True)])
        SparkDD_DF = spark.createDataFrame(DataDict, schema)
    return SparkDD_DF




if noofcolumn !=0:
    if noofcolumn==3:
        print("The Data Dictionary having three fields")
        FirstColName, SecondColName, ThirdColName, Is_first_col_valid, Is_Secod_col_valid = DD_Col_Pattern_validation(noofcolumn, DataDict, FirstColRe, secondColRe, ThirdColRe, FirstColumnName, SecondDataType, ThirdIsNullable)
        if Is_first_col_valid == 1 & Is_Secod_col_valid == 1:
            print("Data Dictionary excel column is in correct order")
            SparkDD_DF= dd_schema_creation(spark, DataDict, FirstColName, SecondColName, ThirdColName, noofcolumn)
            SparkDD_DF.show()
            DynamicSchema=dynnamic_schema_creation(spark, SparkDD_DF, noofcolumn)
            print("Dynamic schema got created as per excel input ")
            print(DynamicSchema)
        else:
            print("Data Dictionary column is not in correct order , it should be column_name first then followed by Data_Type,\nplease correct the data dictionary and process it again \n Thank you")

    else :
        print("The Data Dictionary having two fields")

        FirstColName, SecondColName,ThirdColName, Is_first_col_valid, Is_Secod_col_valid = DD_Col_Pattern_validation(noofcolumn, DataDict, FirstColRe, secondColRe, ThirdColRe, FirstColumnName, SecondDataType, ThirdIsNullable)
        if Is_first_col_valid == 1 & Is_Secod_col_valid == 1:
            print("Data Dictionary excel column is in correct order")
            SparkDD_DF = dd_schema_creation(spark, DataDict, FirstColName, SecondColName, ThirdColName, noofcolumn)
            SparkDD_DF.show()
            DynamicSchema=dynnamic_schema_creation(spark, SparkDD_DF, noofcolumn)
            print("Dynamic schema got created as per excel input ")
            print(DynamicSchema)

# reading file using dynamic schema
csvload = spark.read.csv(inputfilepath, header="False", schema=DynamicSchema)
csvload.show()

