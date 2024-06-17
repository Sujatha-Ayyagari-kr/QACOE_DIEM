# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.master("local[*]").appName("Great Expectations with Pandas DataFrame").getOrCreate()

# COMMAND ----------

# raw_df = spark.read.option("header", True).csv("../dataset/Kickstarter_projects_Feb19.csv")
raw_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/anand.mulgund@kroger.com/Kickstarter_projects_Feb19.csv")
raw_df.createOrReplaceTempView("CAMPAIGNS")

# COMMAND ----------

type(raw_df)

# COMMAND ----------

raw_df.printSchema()

# COMMAND ----------

raw_df.toPandas()

# COMMAND ----------

# MAGIC %pip list
# MAGIC %pip install great-expectations==0.17.12

# COMMAND ----------

from great_expectations.dataset import SparkDFDataset

# COMMAND ----------

raw_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/anand.mulgund@kroger.com/Kickstarter_projects_Feb19.csv")
raw_df.createOrReplaceTempView("CAMPAIGNS")
raw_test_df = SparkDFDataset(raw_df)
print(f"raw_test_df type {type(raw_test_df)}")

# COMMAND ----------

MANDATORY_COLUMNS = [
    "id",
    "currency",
    "main_category",
    "launched_at",
    "deadline",
    "country",
    "status",
    "usd_pledged"
]

# COMMAND ----------

for column in MANDATORY_COLUMNS:
    try:
        assert raw_test_df.expect_column_to_exist(
            column).success, f"Uh oh! Mandatory column {column} does not exist: FAILED"
        print(f"Column {column} exists : PASSED")
    except AssertionError as e:
        print(e)

# COMMAND ----------

for column in MANDATORY_COLUMNS:
    try:
        assert raw_test_df.expect_column_to_exist(
            column).success, f"Uh oh! Mandatory column {column} does not exist: FAILED"
        print(f"Column {column} exists : PASSED")
        assert raw_test_df.expect_column_values_to_not_be_null(column, result_format={"result_format": "COMPLETE"})
        print(f"Column {column} not null : PASSED")
    except AssertionError as e:
        print(e)


# COMMAND ----------

def dq_validate(dq_raw_test_df:SparkDFDataset) -> bool:
    ret = True
    for column in MANDATORY_COLUMNS:
        try:
            assert dq_raw_test_df.expect_column_to_exist(
                column).success, f"Uh oh! Mandatory column {column} does not exist: FAILED"
            #print(f"Column {column} exists : PASSED")
            assert dq_raw_test_df.expect_column_values_to_not_be_null(column, result_format={"result_format": "COMPLETE"})
            #print(f"Column {column} not null : PASSED")
        except AssertionError as e:
            print(e)
    return ret

for i in range(10):
    dq_result = dq_validate(dq_raw_test_df=raw_test_df)
    if (i % 2 == 0):
        print(f"ran dq_validate:{i}")

print(dq_result)
