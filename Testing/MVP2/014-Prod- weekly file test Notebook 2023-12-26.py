# Databricks notebook source
dfg= spark.read.csv("/mnt/gold/release/sales/2023_12_17/csv/101_Weekly_Sales.014.20231217055446.csv",header=True)
dfg.createOrReplaceTempView("gld_vw")

# COMMAND ----------

import fnmatch
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark_session = SparkSession.builder.appName('spark_Session').getOrCreate()
emp_RDD = spark_session.sparkContext.emptyRDD()

columns = StructType([
    StructField('UPC_TYPE', StringType(), nullable=True),
    StructField('DIV', StringType(), nullable=True),
    StructField('STORE', StringType(), nullable=True),
    StructField('ITEM_CONSUMER_UPC', StringType(), nullable=True),
    StructField('POS_DATE', StringType(), nullable=True),
    StructField('POS_NET_DOL_AMOUNT', StringType(), nullable=True),
    StructField('POS_UOM_QTY', StringType(), nullable=True),
    StructField('POS_UNITS', StringType(), nullable=True),
    StructField('POS_UOM', StringType(), nullable=True),
    StructField('ITEM_SRC_DIV', StringType(), nullable=True),
    StructField('ITEM_SRC_LOC', StringType(), nullable=True),
    StructField('MODALITY', StringType(), nullable=True),
    StructField('FULFILLMENT', StringType(), nullable=True),
    StructField('POS_GROSS_DOL_AMOUNT', StringType(), nullable=True),
    StructField('STORE_COUPON_AMOUNT', StringType(), nullable=True),
    StructField('MANUFACTURER_COUPON_AMOUNT', StringType(), nullable=True),
    StructField('index', StringType(), nullable=True)
])

dsdf= spark_session.createDataFrame(data=emp_RDD,schema=columns)

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import lit
from functools import reduce
from pyspark.sql import DataFrame
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
rawFolderpath1="/mnt/data/release/daily_sales/landing_archive/2023_12_14/csv/"
rawFolderpath2="/mnt/data/release/daily_sales/landing_archive/2023_12_15/csv/"
rawFolderpath3="/mnt/data/release/daily_sales/landing_archive/2023_12_16/csv/"
rawFolderpath4="/mnt/data/release/daily_sales/landing_archive/2023_12_17/csv/"
fileListSchema = ['path','name','size']

rawFileList1 = dbutils.fs.ls(rawFolderpath1)
rawList1 = spark.createDataFrame(data=rawFileList1, schema = fileListSchema)
rawList1.createOrReplaceTempView("vw_rawFileList1")

rawFileList2 = dbutils.fs.ls(rawFolderpath2)
rawList2 = spark.createDataFrame(data=rawFileList2, schema = fileListSchema)
rawList2.createOrReplaceTempView("vw_rawFileList2")

rawFileList3 = dbutils.fs.ls(rawFolderpath3)
rawList3 = spark.createDataFrame(data=rawFileList3, schema = fileListSchema)
rawList3.createOrReplaceTempView("vw_rawFileList3")

rawFileList4 = dbutils.fs.ls(rawFolderpath4)
rawList4 = spark.createDataFrame(data=rawFileList4, schema = fileListSchema)
rawList4.createOrReplaceTempView("vw_rawFileList4")


sfile1=spark.sql("select path,name from vw_rawFileList1 where name like '101_Daily_Sales.014.%' union all select path,name from vw_rawFileList2 where name like '101_Daily_Sales.014.%' union all select path,name from vw_rawFileList3 where name like '101_Daily_Sales.014.%' union all select path,name from vw_rawFileList4 where name like '101_Daily_Sales.014.%'")
sfileCollect=sfile1.collect()
for row in sfileCollect:
    sfiledc= str(row["name"])
    spathdc= str(row["path"])
    #print(sfiledc)
    dt = sfiledc[20:28]
    #print(dt)
    cdt=datetime.strptime(dt,'%Y%m%d')
    idx = (cdt.weekday() + 1) % 7 # MON = 0, SUN = 6 -> SUN = 0 .. SAT = 6
    #print(idx)
    sfilepath1=str(spathdc)
    #print("DSA Raw file: " +sfilepath1)
    dfs= spark.read.option("header","true").csv(sfilepath1)
    dfsi = dfs.withColumn("index",lit(idx))
    dsdf = dsdf.union(dfsi)
#dsdf.display()
dsdf.createOrReplaceTempView('vw_union')
restatesql="select * from (select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV,ITEM_SRC_LOC,MODALITY,FULFILLMENT,index, row_number() over(partition by DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,ITEM_SRC_DIV,MODALITY,FULFILLMENT order by index desc) as rn from vw_union) where rn=1 and weekofyear(to_date(POS_DATE,'MM/dd/yyyy')+1)=weekofyear('2023-12-15')"
restatedf=spark.sql(restatesql)
restatedf.createOrReplaceTempView('vw_restate')
restatesql2="select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,case when index=0 then to_date(POS_DATE,'MM/dd/yyyy') else to_date(date_sub(next_day(to_date(POS_DATE,'MM/dd/yyyy'),'SAT'),0),'MM/dd/yyyy') end as POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV, ITEM_SRC_LOC from vw_restate"
restatedf2=spark.sql(restatesql2)
restatedf2.createOrReplaceTempView('vw_restate2')
bronzSql="select * from vw_restate2 where DIV is not null and STORE is not null and ITEM_CONSUMER_UPC is not null and POS_DATE is not null and POS_NET_DOL_AMOUNT is not null and POS_UOM_QTY is not null and POS_UNITS is not null " 
silverDf=spark.sql(bronzSql)
i_df = silverDf.filter("UPC_TYPE='I'")
c_df =  silverDf.filter("UPC_TYPE='C'").join(i_df,["DIV","STORE","ITEM_CONSUMER_UPC"],"left_anti")
combined_df = i_df.unionByName(c_df)
combined_df.createOrReplaceTempView('vwSilverData')
rollupSql="SELECT  DIV,STORE, ITEM_CONSUMER_UPC, max(POS_DATE) as POS_DATE,SUM(POS_NET_DOL_AMOUNT) AS POS_NET_DOL_AMOUNT, SUM(POS_UOM_QTY) AS POS_UOM_QTY, SUM(POS_UNITS) AS POS_UNITS, MAX(POS_UOM) AS POS_UOM,ITEM_SRC_DIV from vwSilverData GROUP BY  DIV,STORE, ITEM_CONSUMER_UPC,ITEM_SRC_DIV"
weeklydf = spark.sql(rollupSql)
#display(weeklydf)
weeklydf.createOrReplaceTempView('src_vw')


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark_session = SparkSession.builder.appName('spark_Session').getOrCreate()
emp_RDD = spark_session.sparkContext.emptyRDD()
columns = StructType([
                      StructField('TestCaseSummary',StringType(),False),
                      StructField('TestCaseDescription',StringType(),False),
                      StructField('Status', StringType(), False),
                      StructField('Comments', StringType(), False),
                      StructField('Test_Data', StringType(), False)
                      ])
testResult = spark_session.createDataFrame(data=emp_RDD,schema=columns)


# COMMAND ----------

# MAGIC %md #Count Validation

# COMMAND ----------

dfsrccnt=spark.sql("select count(1) from src_vw minus select count(1) from gld_vw union all select count(1) from gld_vw minus select count(1) from src_vw")
dfsrccountunion=spark.sql("select 'src' as tabnm, count(1) from src_vw union all select 'gld' as tabnm, count(1) from gld_vw")
dfrowcount=dfsrccnt.count()
testResult=[]
if dfrowcount<1:
    testResult.append(('Count comparison rolled_up raw file vs ETL Ready file', 'Pass', 'Record count is matching'))
else:
    testResult.append(('Count comparison raw file vs ETL Ready file', 'Fail', 'Record count is not matching, please check dfsrccountunion'))
display(dfsrccountunion)   

# COMMAND ----------

# MAGIC %md #Aggregate Validation

# COMMAND ----------

dfaggdiff=spark.sql("select  sum(cast(POS_NET_DOL_AMOUNT as decimal(8,3))) , sum(cast(POS_UOM_QTY as decimal(18,4))), sum(POS_UNITS) from src_vw minus select sum(cast(POS_NET_DOL_AMOUNT as decimal(8,3))),sum(cast(POS_UOM_QTY as decimal(18,4))), sum(POS_UNITS) from gld_vw")
dfsaggunion=spark.sql("select 'src' as tabnm, sum(cast(POS_NET_DOL_AMOUNT as decimal(8,3))),sum(cast(POS_UOM_QTY as decimal(18,4))), sum(POS_UNITS) from src_vw union all select 'gld' as tabnm, sum(cast(POS_NET_DOL_AMOUNT as decimal(8,3))),sum(cast(POS_UOM_QTY as decimal(18,4))), sum(POS_UNITS) from gld_vw")
dfaggcount=dfaggdiff.count()
if dfaggcount<1:
    testResult.append(('Aggregate validation rolled up raw file vs ETL Ready file', 'Pass', 'Sum of Amount and unit values are matching'))
else:
    testResult.append(('Aggregate validation rolled up raw file vs ETL Ready file', 'Fail', 'Sum of Amount and unit values are not matching please refer to dfsaggunion'))
display(dfsaggunion)  

# COMMAND ----------

# MAGIC %md #Minus query

# COMMAND ----------

dfsminust=spark.sql("select DIV,STORE,ITEM_CONSUMER_UPC,cast(POS_NET_DOL_AMOUNT as decimal (8,3)),cast(POS_UOM_QTY as decimal (4,2)),cast(POS_UNITS as decimal),POS_UOM from src_vw minus select DIV,STORE,ITEM_CONSUMER_UPC,cast(POS_NET_DOL_AMOUNT as decimal (8,3)),cast(POS_UOM_QTY as decimal (4,2)),cast(POS_UNITS as decimal),POS_UOM from gld_vw")
dftminuss=spark.sql("select DIV,STORE,ITEM_CONSUMER_UPC,cast(POS_NET_DOL_AMOUNT as decimal (8,3)),cast(POS_UOM_QTY as decimal (4,2)),cast(POS_UNITS as decimal),POS_UOM from gld_vw minus select DIV,STORE,ITEM_CONSUMER_UPC,cast(POS_NET_DOL_AMOUNT as decimal (8,3)),cast(POS_UOM_QTY as decimal (4,2)),cast(POS_UNITS as decimal),POS_UOM from src_vw")
dfminusunion=dfsminust.union(dftminuss)
dfminuscount=dfminusunion.count()
if dfminuscount<1:
    testResult.append(('Minus query rolled-up raw file vs ETL Ready file', 'Pass', 'Data matches raw file vs ETL Ready file'))
else:
    testResult.append(('Minus query rolled-up raw file vs ETL Ready file', 'Fail', 'Data does not match, please check dfminusunion'))
    display(dfminusunion)

# COMMAND ----------

# MAGIC %md #Duplicate check on Gold data

# COMMAND ----------

dfdup=spark.sql("select DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV from gld_vw group by DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV having count(1)>2")
dfdupcount=dfdup.count()
if dfdupcount<1:
    testResult.append(('Duplicate check', 'Pass', 'No duplicate records'))
else:
    testResult.append(('Duplicate check', 'Fail', 'Duplicate records are foundcheck dfdup'))
    

# COMMAND ----------

# MAGIC %md #Result Dashboard

# COMMAND ----------

dfResult = spark_session.createDataFrame (data=testResult, schema=columns)
dfResult.createOrReplaceTempView("testResult_vw")
dfResult.display()

# COMMAND ----------

import pandas as pd

testcases_data = [
("testcase1","Count comparison vs target","Counts in both the dataframe must be equal","","high","kon7198","count check","Count are equal in source and target","DEART-23755"),
("testcase2","Aggregate validation","Aggregate validation rolled up raw file vs ETL Ready file","","high","kon7198","aggregate validation","Sum of Amount and unit values are matching","DEART-23755"),
("testcase3","Column level data validation","Column level data validation source vs target","","high","kon7198","full validation check","Values in source and target dataframe are matching","DEART-23755"),
("testcase4","Check for dupliacte records","Duplicates should not be present","","high","kon7198","duplicate check","No duplicates are present","DEART-23755")
]

testcases_schema = ["TestCases", "Summary", "Description", "TestStatus", "Priority", "Assignee", "Test_Data", "Expected_Result", "JIRA"] 

df_testcases = spark.createDataFrame(testcases_data, testcases_schema)

df_testcases.display()

# COMMAND ----------

testresults_df = dfResult.join(df_testcases, on = "Test_Data", how = "left")
testresults_df.display()

# COMMAND ----------

import pandas as pd
import os

import json
from pyspark.dbutils import DBUtils

testresults_df1 = testresults_df.select("testcases", "summary", "description", "status", "priority", "assignee", "test_data", "expected_result", "jira")
#testresults_df1.display()

#Load Test results in Json format:
json_data = testresults_df1.toJSON().collect()
#print(json_data,"\n")

testresults_json = {}

for json_string in json_data:
    json_object = json.loads(json_string)
    test_case =json_object['testcases']
    del json_object['testcases']
    testresults_json[test_case] = json_object

#print(testresults_json,"\n")

#Show Test Results in Json format:
print(json.dumps(testresults_json, indent=4))

#Load Test Results in Json file: to databricks default directory: /databricks/driver

with open('testresults_json_file', 'w') as json_file:
    json.dump(testresults_json, json_file, indent=4)

print("Current working directory:", os.getcwd())
#testresults_json_file1.json

