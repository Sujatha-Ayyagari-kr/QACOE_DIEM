# Databricks notebook source
# MAGIC %fs ls /mnt/data/manonmani.varadharajan@kroger.com/daily_sales/landing/csv/

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
dsdf.display()
dsdf.createOrReplaceTempView('vw_union')

# COMMAND ----------



# COMMAND ----------

restatesql="select * from (select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV,ITEM_SRC_LOC,MODALITY,FULFILLMENT,index, row_number() over(partition by DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,ITEM_SRC_DIV,MODALITY,FULFILLMENT order by index desc) as rn from vw_union) where rn=1 and weekofyear(to_date(POS_DATE,'MM/dd/yyyy')+1)=weekofyear('2023-09-16')"
restatedf=spark.sql(restatesql)
restatedf.createOrReplaceTempView('vw_restate')

# COMMAND ----------

# MAGIC %sql select count(1) from vw_union
# MAGIC union all
# MAGIC select count(1) from vw_restate

# COMMAND ----------

# MAGIC %sql
# MAGIC select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV,ITEM_SRC_LOC,MODALITY,FULFILLMENT from vw_restate
# MAGIC
# MAGIC minus
# MAGIC select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV,ITEM_SRC_LOC,MODALITY,FULFILLMENT from vw_union

# COMMAND ----------

# MAGIC %sql
# MAGIC select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV,ITEM_SRC_LOC,MODALITY,FULFILLMENT from vw_union
# MAGIC minus
# MAGIC select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV,ITEM_SRC_LOC,MODALITY,FULFILLMENT from vw_restate

# COMMAND ----------

# MAGIC %sql select distinct POS_DATE from vw_union

# COMMAND ----------

# MAGIC %sql select distinct POS_DATE from vw_restate

# COMMAND ----------

# MAGIC %sql select * from vw_union where ITEM_CONSUMER_UPC='0000000004032'

# COMMAND ----------

# MAGIC %sql select * from vw_restate where ITEM_CONSUMER_UPC='0000000004032'

# COMMAND ----------

# MAGIC %sql select * from vw_union where ITEM_CONSUMER_UPC='0000000023456'
# MAGIC

# COMMAND ----------

# MAGIC %sql select * from vw_restate where ITEM_CONSUMER_UPC='0000000023456'

# COMMAND ----------

# MAGIC %sql select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,POS_DATE,case when index=0 then POS_DATE else date_sub(next_day(to_date(POS_DATE,'MM/dd/yyyy'),'SAT'),0) end as POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV, ITEM_SRC_LOC from vw_restate

# COMMAND ----------

restatesql2="select UPC_TYPE,DIV,STORE,ITEM_CONSUMER_UPC,case when index=0 then POS_DATe else date_sub(next_day(to_date(POS_DATE,'MM/dd/yyyy'),'SAT'),0) end as POS_DATE,POS_NET_DOL_AMOUNT,POS_UOM_QTY,POS_UNITS,POS_UOM,ITEM_SRC_DIV, ITEM_SRC_LOC from vw_restate"
restatedf2=spark.sql(restatesql2)
restatedf2.createOrReplaceTempView('vw_restate2')

# COMMAND ----------

# MAGIC %sql select count(1) from vw_restate2

# COMMAND ----------

bronzSql="select * from vw_restate2 where DIV is not null and STORE is not null  and ITEM_CONSUMER_UPC is not null and POS_DATE is not null and POS_NET_DOL_AMOUNT is not null and POS_UOM_QTY is not null and POS_UNITS is not null " 
silverDf=spark.sql(bronzSql)
silverDf.createOrReplaceTempView('vwBronzData')
i_df = silverDf.filter("UPC_TYPE='I'")
c_df =  silverDf.filter("UPC_TYPE='C'").join(i_df,["DIV","STORE","ITEM_CONSUMER_UPC"],"left_anti")
#display(c_df)
combined_df = i_df.unionByName(c_df)
combined_df.createOrReplaceTempView('vwSilverData')
rollupSql="SELECT  DIV,STORE, ITEM_CONSUMER_UPC, POS_DATE,SUM(POS_NET_DOL_AMOUNT) AS POS_NET_DOL_AMOUNT, SUM(POS_UOM_QTY) AS POS_UOM_QTY, SUM(POS_UNITS) AS POS_UNITS, MAX(POS_UOM) AS POS_UOM,ITEM_SRC_DIV from vwSilverData GROUP BY  DIV,STORE, ITEM_CONSUMER_UPC, POS_DATE, ITEM_SRC_DIV"
trollup_df = spark.sql(rollupSql)
display(trollup_df)

# COMMAND ----------

# MAGIC %sql select count(1) from vwBronzData
