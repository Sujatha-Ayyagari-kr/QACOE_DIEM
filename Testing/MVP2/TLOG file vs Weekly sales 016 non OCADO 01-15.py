# Databricks notebook source
# MAGIC %fs ls /mnt/data/manonmani.varadharajan@kroger.com/weekly_sales/

# COMMAND ----------

df= spark.read.load("/mnt/data/manonmani.varadharajan@kroger.com/weekly_sales/tlo0047_fcb_non_ace_movement_016.dat.20240107085327",format="csv",header="false")
df.createOrReplaceTempView('tlogfile')

# COMMAND ----------

# MAGIC %sql select * from tlogfile where substr(_c0,9,1)='M'

# COMMAND ----------

# MAGIC %sql
# MAGIC select substr(_c0,1,3) as KMA_NO,substr(_c0,4,5) as STO_NO, substr(_c0,9,1) as RCD_ID,  substr(_c0,10,10) as GNC_EFF_DT,substr(_c0,20,13) as SCN_UPC_NO, substr(_c0,33,1) as SCN_MOV_SN, substr(_c0,34,7) as SCN_MOV_QY, substr(_c0,41,1) as WGT_SN, substr(_c0,42,9) as WGT_QY, substr(_c0,51,1) as SCN_RTL_SN, substr(_c0,52,9) as SCN_RTL_AM  from tlogfile where substr(_c0,9,1)='M'

# COMMAND ----------

# MAGIC %sql select substr(_c0,1,3) as KMA_NO,substr(_c0,4,5) as STO_NO,  substr(_c0,10,10) as GNC_EFF_DT,substr(_c0,20,13) as SCN_UPC_NO,  substr(_c0,34,7) as SCN_MOV_QY, substr(_c0,42,9) as WGT_QY,  substr(_c0,52,9) as SCN_RTL_AM  from tlogfile where substr(_c0,9,1)='M'

# COMMAND ----------

dfe= spark.read.csv("/mnt/data/manonmani.varadharajan@kroger.com/weekly_sales/Expected_weekly_016_01-07.csv",header=True)
dfe.createOrReplaceTempView("weeklyfile")

# COMMAND ----------

# MAGIC %sql select distinct DIV, ITEM_SRC_DIV from weeklyfile

# COMMAND ----------

dfe2=spark.sql("select DIV,STORE,ITEM_CONSUMER_UPC,sum(POS_NET_DOL_AMOUNT) as POS_NET_DOL_AMOUNT,sum(POS_UOM_QTY) as POS_UOM_QTY,sum(POS_UNITS) as POS_UNITS from weeklyfile where ITEM_SRC_DIV<>'540' and ITEM_SRC_DIV is not null and ITEM_CONSUMER_UPC<>'0000000000000' and (POS_NET_DOL_AMOUNT <>0 or POS_UOM_QTY<>0 or  POS_UNITS<>0) group by DIV,STORE,ITEM_CONSUMER_UPC")
dfe2.createOrReplaceTempView("weeklyfile2")

# COMMAND ----------

df2=spark.sql("select substr(_c0,1,3) as KMA_NO,substr(_c0,4,5) as STO_NO,  substr(_c0,10,10) as GNC_EFF_DT,substr(_c0,20,13) as SCN_UPC_NO,  substr(_c0,34,7) as SCN_MOV_QY, substr(_c0,42,9) as WGT_QY,  substr(_c0,52,9) as SCN_RTL_AM  from tlogfile where substr(_c0,9,1)='M'")
df2.createOrReplaceTempView("tlogfile2")

# COMMAND ----------

# MAGIC %sql select *  from tlogfile2

# COMMAND ----------

# MAGIC %sql select 'tlog',count(1)  from tlogfile2 
# MAGIC union all
# MAGIC select 'Weekly',count(1) from weeklyfile2 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(tab) as tab,STORE ,POS_UNITS, POS_UOM_QTY,POS_NET_DOL_AMOUNT  from
# MAGIC (
# MAGIC select 'tlog' as tab,STO_NO as STORE ,sum(cast(SCN_MOV_QY as integer)) as POS_UNITS,sum(WGT_QY)/100 as POS_UOM_QTY,sum(SCN_RTL_AM)/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where (SCN_RTL_AM <>0 or WGT_QY<>0 or  SCN_MOV_QY<>0) group by STO_NO
# MAGIC union all
# MAGIC select 'weekly' as tab,STORE,sum(cast(replace(POS_UNITS,'-','') as integer)),round(sum(replace(POS_UOM_QTY,'-','')),2),round(sum(replace(POS_NET_DOL_AMOUNT,'-','')),2) from weeklyfile2 where (POS_NET_DOL_AMOUNT <>0 or POS_UOM_QTY<>0 or  POS_UNITS<>0)  group by STORE
# MAGIC ) group by STORE ,POS_UNITS, POS_UOM_QTY,POS_NET_DOL_AMOUNT
# MAGIC having count(1)<>2
# MAGIC order by STORE

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(tab) as tab,ITEM_CONSUMER_UPC ,POS_NET_DOL_AMOUNT  from
# MAGIC (
# MAGIC select 'tlog' as tab,SCN_UPC_NO as ITEM_CONSUMER_UPC ,sum(SCN_RTL_AM)/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where (SCN_RTL_AM <>0 or WGT_QY<>0 or  SCN_MOV_QY<>0) group by SCN_UPC_NO
# MAGIC union all
# MAGIC select 'weekly' as tab,ITEM_CONSUMER_UPC,round(sum(replace(POS_NET_DOL_AMOUNT,'-','')),2) from weeklyfile2 where (POS_NET_DOL_AMOUNT <>0 or POS_UOM_QTY<>0 or  POS_UNITS<>0)  group by ITEM_CONSUMER_UPC
# MAGIC ) group by ITEM_CONSUMER_UPC ,POS_NET_DOL_AMOUNT
# MAGIC having count(1)<>2
# MAGIC order by ITEM_CONSUMER_UPC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(tab) as tab,ITEM_CONSUMER_UPC ,POS_UNITS, POS_UOM_QTY,POS_NET_DOL_AMOUNT  from
# MAGIC (
# MAGIC select 'tlog' as tab,SCN_UPC_NO as ITEM_CONSUMER_UPC ,sum(cast(SCN_MOV_QY as integer)) as POS_UNITS,sum(WGT_QY)/100 as POS_UOM_QTY,sum(SCN_RTL_AM)/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where (SCN_RTL_AM <>0 or WGT_QY<>0 or  SCN_MOV_QY<>0) group by SCN_UPC_NO
# MAGIC union all
# MAGIC select 'weekly' as tab,ITEM_CONSUMER_UPC,sum(cast(replace(POS_UNITS,'-','') as integer)),round(sum(replace(POS_UOM_QTY,'-','')),2),round(sum(replace(POS_NET_DOL_AMOUNT,'-','')),2) from weeklyfile2 where (POS_NET_DOL_AMOUNT <>0 or POS_UOM_QTY<>0 or  POS_UNITS<>0)  group by ITEM_CONSUMER_UPC
# MAGIC ) group by ITEM_CONSUMER_UPC ,POS_UNITS, POS_UOM_QTY,POS_NET_DOL_AMOUNT
# MAGIC having count(1)<>2
# MAGIC order by ITEM_CONSUMER_UPC

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC
# MAGIC select 'tlog' as tab,sum(SCN_RTL_AM)/100 as POS_NET_DOL_AMOUNT   from tlogfile2 where (SCN_RTL_AM <>0 or WGT_QY<>0 or  SCN_MOV_QY<>0) and SCN_UPC_NO='0000000005919' union all
# MAGIC select 'weekly' as tab,round(sum(replace(POS_NET_DOL_AMOUNT,'-','')),2) from weeklyfile2 where (POS_NET_DOL_AMOUNT <>0 or POS_UOM_QTY<>0 or  POS_UNITS<>0) and ITEM_CONSUMER_UPC='0000000005919'

# COMMAND ----------

# MAGIC %sql select 'tlog', STO_NO as STORE,SCN_UPC_NO as ITEM_CONSUMER_UPC ,cast(SCN_MOV_QY as INTEGER) as  POS_UNITS,WGT_QY/100 as POS_UOM_QTY,SCN_RTL_AM/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where SCN_UPC_NO='0000000005919' 
# MAGIC union all
# MAGIC select 'weekly',STORE,ITEM_CONSUMER_UPC,cast(POS_UNITS as integer),round(POS_UOM_QTY,2),replace(round(POS_NET_DOL_AMOUNT,2),'-','') from weeklyfile2  where ITEM_CONSUMER_UPC='0000000005919' 
# MAGIC ORDER BY STORE

# COMMAND ----------

# MAGIC %sql 
# MAGIC select min(tab) as tab,ITEM_CONSUMER_UPC ,POS_NET_DOL_AMOUNT  from
# MAGIC (
# MAGIC select 'tlog' as tab,SCN_UPC_NO as ITEM_CONSUMER_UPC ,sum(SCN_RTL_AM)/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where (SCN_RTL_AM <>0 or WGT_QY<>0 or  SCN_MOV_QY<>0) and STO_NO='00232' and SCN_UPC_NO='0000000005919' group by SCN_UPC_NO
# MAGIC union all
# MAGIC select 'weekly' as tab,ITEM_CONSUMER_UPC,round(sum(replace(POS_NET_DOL_AMOUNT,'-','')),2) from weeklyfile2 where (POS_NET_DOL_AMOUNT <>0 or POS_UOM_QTY<>0 or  POS_UNITS<>0) and STORE='00232' and ITEM_CONSUMER_UPC='0000000005919' group by ITEM_CONSUMER_UPC
# MAGIC ) group by ITEM_CONSUMER_UPC ,POS_NET_DOL_AMOUNT
# MAGIC having count(1)<>2
# MAGIC order by ITEM_CONSUMER_UPC

# COMMAND ----------

# MAGIC %sql select *  from tlogfile2 where SCN_UPC_NO='0000000005919' and STO_NO='00232'

# COMMAND ----------

# MAGIC %sql select *  from weeklyfile2 where ITEM_CONSUMER_UPC='0000000005919' and STORE='00232'

# COMMAND ----------

# MAGIC %sql select * from (
# MAGIC select min(tab) as tab,ITEM_CONSUMER_UPC   from
# MAGIC (
# MAGIC select 'tlog' as tab,SCN_UPC_NO as ITEM_CONSUMER_UPC  from tlogfile2 where (SCN_RTL_AM <>0 or WGT_QY<>0 or  SCN_MOV_QY<>0) and STO_NO='00522' group by SCN_UPC_NO
# MAGIC union all
# MAGIC select 'weekly' as tab,ITEM_CONSUMER_UPC from weeklyfile2 where (POS_NET_DOL_AMOUNT <>0 or POS_UOM_QTY<>0 or  POS_UNITS<>0) and STORE='00522' group by ITEM_CONSUMER_UPC
# MAGIC ) group by ITEM_CONSUMER_UPC 
# MAGIC having count(1)<>2
# MAGIC order by ITEM_CONSUMER_UPC) where tab='weekly'

# COMMAND ----------

# MAGIC %sql select 'tlog', STO_NO as STORE,SCN_UPC_NO as ITEM_CONSUMER_UPC ,cast(SCN_MOV_QY as INTEGER) as  POS_UNITS,WGT_QY/100 as POS_UOM_QTY,SCN_RTL_AM/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where SCN_UPC_NO='0010000010017' and STO_NO='00522'
# MAGIC union all
# MAGIC select 'weekly',STORE,ITEM_CONSUMER_UPC,cast(POS_UNITS as integer),round(POS_UOM_QTY,2),replace(round(POS_NET_DOL_AMOUNT,2),'-','') from weeklyfile2  where ITEM_CONSUMER_UPC='0010000010017' and store='00522'
# MAGIC ORDER BY STORE

# COMMAND ----------

# MAGIC %sql select *  from tlogfile2 where SCN_UPC_NO='0800000021972' and STO_NO='00522'

# COMMAND ----------

# MAGIC %sql select 'tlog', STO_NO as STORE,SCN_UPC_NO as ITEM_CONSUMER_UPC ,cast(SCN_MOV_QY as INTEGER) as  POS_UNITS,WGT_QY/100 as POS_UOM_QTY,SCN_RTL_AM/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where SCN_UPC_NO='0010000030263' and STO_NO='00522'
# MAGIC union all
# MAGIC select 'weekly',STORE,ITEM_CONSUMER_UPC,cast(POS_UNITS as integer),round(POS_UOM_QTY,2),replace(round(POS_NET_DOL_AMOUNT,2),'-','') from weeklyfile2  where ITEM_CONSUMER_UPC='0010000030263' and store='00522'
# MAGIC ORDER BY STORE

# COMMAND ----------

# MAGIC %sql select *  from tlogfile2 where SCN_UPC_NO='0010000030263' and STO_NO='00522'

# COMMAND ----------

# MAGIC %sql select 'tlog', STO_NO as STORE,SCN_UPC_NO as ITEM_CONSUMER_UPC ,cast(SCN_MOV_QY as INTEGER) as  POS_UNITS,WGT_QY/100 as POS_UOM_QTY,SCN_RTL_AM/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where SCN_UPC_NO='0800000262937' and STO_NO='00522'
# MAGIC union all
# MAGIC select 'weekly',STORE,ITEM_CONSUMER_UPC,cast(POS_UNITS as integer),round(POS_UOM_QTY,2),replace(round(POS_NET_DOL_AMOUNT,2),'-','') from weeklyfile2  where ITEM_CONSUMER_UPC='0800000262937' and store='00522'
# MAGIC ORDER BY STORE

# COMMAND ----------

# MAGIC %sql select *  from tlogfile2 where SCN_UPC_NO='0800000262937' and STO_NO='00522'

# COMMAND ----------

# MAGIC %sql select 'tlog', STO_NO as STORE,SCN_UPC_NO as ITEM_CONSUMER_UPC ,cast(SCN_MOV_QY as INTEGER) as  POS_UNITS,WGT_QY/100 as POS_UOM_QTY,SCN_RTL_AM/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where SCN_UPC_NO='0000000005919' and STO_NO='00522'
# MAGIC union all
# MAGIC select 'weekly',STORE,ITEM_CONSUMER_UPC,cast(POS_UNITS as integer),round(POS_UOM_QTY,2),replace(round(POS_NET_DOL_AMOUNT,2),'-','') from weeklyfile2  where ITEM_CONSUMER_UPC='0000000005919' and store='00522'
# MAGIC ORDER BY STORE

# COMMAND ----------

# MAGIC %sql select 'tlog', STO_NO as STORE,SCN_UPC_NO as ITEM_CONSUMER_UPC ,cast(SCN_MOV_QY as INTEGER) as  POS_UNITS,WGT_QY/100 as POS_UOM_QTY,SCN_RTL_AM/100 as POS_NET_DOL_AMOUNT from tlogfile2 where SCN_UPC_NO='0000000023456' and STO_NO='00522'
# MAGIC union all
# MAGIC select 'weekly',STORE,ITEM_CONSUMER_UPC,cast(POS_UNITS as integer),round(POS_UOM_QTY,2),replace(round(POS_NET_DOL_AMOUNT,2),'-','') from weeklyfile2 where ITEM_CONSUMER_UPC='0000000023456' and store='00522'

# COMMAND ----------

# MAGIC %sql select 'tlog', STO_NO as STORE,SCN_UPC_NO as ITEM_CONSUMER_UPC ,cast(SCN_MOV_QY as INTEGER) as  POS_UNITS,WGT_QY/100 as POS_UOM_QTY,SCN_RTL_AM/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where SCN_UPC_NO='0010000015493' and STO_NO='00522'
# MAGIC union all
# MAGIC select 'weekly',STORE,ITEM_CONSUMER_UPC,cast(POS_UNITS as integer),round(POS_UOM_QTY,2),replace(round(POS_NET_DOL_AMOUNT,2),'-','') from weeklyfile2  where ITEM_CONSUMER_UPC='0010000015493' and STORE='00522'
# MAGIC ORDER BY STORE

# COMMAND ----------

# MAGIC %sql select * from weeklyfile where ITEM_CONSUMER_UPC='0877448010893' --and store='00439'

# COMMAND ----------

# MAGIC %sql select * from weeklyfile where ITEM_CONSUMER_UPC='0888109100341'

# COMMAND ----------

# MAGIC %sql select 'tlog', STO_NO as STORE,SCN_UPC_NO as ITEM_CONSUMER_UPC ,cast(SCN_MOV_QY as INTEGER) as  POS_UNITS,WGT_QY/100 as POS_UOM_QTY,SCN_RTL_AM/100 as POS_NET_DOL_AMOUNT  from tlogfile2 where SCN_UPC_NO='0811099999999'
# MAGIC union all
# MAGIC select 'weekly',STORE,ITEM_CONSUMER_UPC,cast(POS_UNITS as integer),round(POS_UOM_QTY,2),replace(round(POS_NET_DOL_AMOUNT,2),'-','') from weeklyfile2  where ITEM_CONSUMER_UPC='0811099999999'
# MAGIC ORDER BY STORE
