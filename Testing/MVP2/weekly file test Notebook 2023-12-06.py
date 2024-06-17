# Databricks notebook source
# MAGIC %fs ls /mnt/data/manonmani.varadharajan@kroger.com/weekly_sales/101_Weekly_Sales.708.20231202024527-1.csv

# COMMAND ----------

dfg= spark.read.csv("/mnt/data/manonmani.varadharajan@kroger.com/weekly_sales/101_Weekly_Sales.708.20231202024527-1.csv",header=True)
dfg.createOrReplaceTempView("gld_vw")

# COMMAND ----------

# dbutils.widgets.removeAll()
# dbutils.widgets.text("Raw_File_Path","/mnt/data/load_date=2023-07-17/101_Daily_Sales.705.20230606062318.csv")
# rawFilePath=dbutils.widgets.get("Raw_File_Path")
# dbutils.widgets.text("Gold_File_Path","/mnt/gold/101_Daily_Sales.705.20230606062318.csv")
# goldFilePath=dbutils.widgets.get("Gold_File_Path")

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

# MAGIC %fs ls /mnt/data/manonmani.varadharajan@kroger.com/daily_sales/landing/csv/

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import lit
from functools import reduce
from pyspark.sql import DataFrame
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
rawFolderpath="/mnt/data/manonmani.varadharajan@kroger.com/daily_sales/landing/csv/"

fileListSchema = ['path','name','size']
rawFileList = dbutils.fs.ls(rawFolderpath)
rawList = spark.createDataFrame(data=rawFileList, schema = fileListSchema)
rawList.createOrReplaceTempView("vw_rawFileList")
sfile1=spark.sql("select path,name from vw_rawFileList where name like '101_Daily_Sales.708.%'")
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
restatesql="select * from (select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV,ITEM_SRC_LOC,MODALITY,FULFILLMENT,index, row_number() over(partition by DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,ITEM_SRC_DIV,MODALITY,FULFILLMENT order by index desc) as rn from vw_union) where rn=1 and weekofyear(to_date(POS_DATE,'MM/dd/yyyy')+1)=weekofyear('2023-11-30')"
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
gldFolderpath="/mnt/data/manonmani.varadharajan@kroger.com/daily_sales/landing/csv/"

# COMMAND ----------

# MAGIC %sql select * from vw_restate

# COMMAND ----------

# MAGIC %sql select * from vwSilverData
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark_session = SparkSession.builder.appName('spark_Session').getOrCreate()
emp_RDD = spark_session.sparkContext.emptyRDD()
columns = StructType([StructField('Test Case Description',StringType(),False),
                      StructField('Test Result', StringType(), False),
                      StructField('Comments', StringType(), False)])
testResult = spark_session.createDataFrame(data=emp_RDD,schema=columns)


# COMMAND ----------

# MAGIC %sql select * from gld_vw

# COMMAND ----------

# MAGIC %sql select * from src_vw

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

# MAGIC %sql select * from gld_vw where ITEM_CONSUMER_UPC like '%10000030903%'

# COMMAND ----------

# MAGIC %sql select 'raw' as tabnm,DIV,STORE,ITEM_CONSUMER_UPC,cast(POS_NET_DOL_AMOUNT as decimal (8,3)),cast(POS_UOM_QTY as decimal (4,2)),cast(POS_UNITS as decimal),POS_UOM,ITEM_SRC_DIV from src_vw where ITEM_CONSUMER_UPC like '%4032%'
# MAGIC union all select 'gld' as tabnm,DIV,STORE,ITEM_CONSUMER_UPC,cast(POS_NET_DOL_AMOUNT as decimal (8,3)),cast(POS_UOM_QTY as decimal (4,2)),cast(POS_UNITS as decimal),POS_UOM,ITEM_SRC_DIV from gld_vw where ITEM_CONSUMER_UPC like '%4032%'

# COMMAND ----------

# MAGIC %sql select min(tabnm) as tabnm, DIV,STORE,ITEM_CONSUMER_UPC,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV, count(1)
# MAGIC from(
# MAGIC select 'raw' as tabnm,DIV,STORE,ITEM_CONSUMER_UPC,cast(POS_NET_DOL_AMOUNT as decimal (8,3)),cast(POS_UOM_QTY as decimal (4,2)),cast(POS_UNITS as decimal),POS_UOM,ITEM_SRC_DIV from src_vw union all select 'gld' as tabnm,lpad(DIV,3,'0'),lpad(STORE,5,'0'),lpad(ITEM_CONSUMER_UPC,13,'0'),cast(POS_NET_DOL_AMOUNT as decimal (8,3)),cast(POS_UOM_QTY as decimal (4,2)),cast(POS_UNITS as decimal),POS_UOM,ITEM_SRC_DIV from gld_vw
# MAGIC ) group by DIV,STORE,ITEM_CONSUMER_UPC,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV
# MAGIC having count(1)<>2
# MAGIC order by STORE,ITEM_CONSUMER_UPC,POS_NET_DOL_AMOUNT

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


