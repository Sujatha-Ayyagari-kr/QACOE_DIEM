# Databricks notebook source
# MAGIC %fs ls /mnt/data/manonmani.varadharajan@kroger.com/weekly_sales/

# COMMAND ----------

df= spark.read.load("/mnt/data/manonmani.varadharajan@kroger.com/weekly_sales/tlo0047_fcb_non_ace_movement_OCADO_026.dat.20231203073827",format="csv",header="false")
df.createOrReplaceTempView('tlogfile')

# COMMAND ----------

# MAGIC %sql select * from tlogfile where substr(_c0,9,1)='M'

# COMMAND ----------

# MAGIC %sql
# MAGIC select substr(_c0,1,3) as KMA_NO,substr(_c0,4,5) as STO_NO, substr(_c0,9,1) as RCD_ID,  substr(_c0,10,10) as GNC_EFF_DT,substr(_c0,20,13) as SCN_UPC_NO, substr(_c0,33,1) as SCN_MOV_SN, substr(_c0,34,7) as SCN_MOV_QY, substr(_c0,41,1) as WGT_SN, substr(_c0,42,9) as WGT_QY, substr(_c0,51,1) as SCN_RTL_SN, substr(_c0,52,9) as SCN_RTL_AM  from tlogfile where substr(_c0,9,1)='M'

# COMMAND ----------

# MAGIC %sql select substr(_c0,1,3) as KMA_NO,substr(_c0,4,5) as STO_NO,  substr(_c0,10,10) as GNC_EFF_DT,substr(_c0,20,13) as SCN_UPC_NO,  substr(_c0,34,7) as SCN_MOV_QY, substr(_c0,42,9) as WGT_QY,  substr(_c0,52,9) as SCN_RTL_AM  from tlogfile where substr(_c0,9,1)='M'

# COMMAND ----------

dfg= spark.read.csv("/mnt/data/manonmani.varadharajan@kroger.com/weekly_sales/101_Weekly_Sales.708.20231202024527-1.csv",header=True)
dfg.createOrReplaceTempView("weeklyfile")

# COMMAND ----------

# MAGIC %sql select DIV,STORE,POS_DATE,ITEM_CONSUMER_UPC,POS_UNITS,POS_UOM_QTY,POS_NET_DOL_AMOUNT from weeklyfile

# COMMAND ----------

# MAGIC %sql select * from weeklyfile

# COMMAND ----------

df2=spark.sql("select substr(_c0,1,3) as KMA_NO,substr(_c0,4,5) as STO_NO,  substr(_c0,10,10) as GNC_EFF_DT,substr(_c0,20,13) as SCN_UPC_NO,  substr(_c0,34,7) as SCN_MOV_QY, substr(_c0,42,9) as WGT_QY,  substr(_c0,52,9) as SCN_RTL_AM  from tlogfile where substr(_c0,9,1)='M'")
df2.createOrReplaceTempView("tlogfile2")

# COMMAND ----------

# MAGIC %sql select KMA_NO,STO_NO,GNC_EFF_DT,SCN_UPC_NO,SCN_MOV_QY,WGT_QY,SCN_RTL_AM  from tlogfile2
# MAGIC minus
# MAGIC select DIV,STORE,POS_DATE,ITEM_CONSUMER_UPC,POS_UNITS,POS_UOM_QTY,POS_NET_DOL_AMOUNT from weeklyfile

# COMMAND ----------

# MAGIC %sql select KMA_NO,STO_NO,GNC_EFF_DT,SCN_UPC_NO,SCN_MOV_QY,WGT_QY,SCN_RTL_AM  from tlogfile2
# MAGIC minus
# MAGIC select DIV,STORE,POS_DATE,ITEM_CONSUMER_UPC,POS_UNITS,POS_UOM_QTY,POS_NET_DOL_AMOUNT from weeklyfile
