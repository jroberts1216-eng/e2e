# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline

# COMMAND ----------

# Library Declarations

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Table

# COMMAND ----------

# Expectations
my_rules = {
    "rule1" : "product_id is not null",
    "rule2" : "product_name is not null"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage():  
    df = spark.readStream.table("databricks_cata.silver.products_silver")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming View

# COMMAND ----------

@dlt.view

def DimProducts_View():

    df  = spark.readStream.table("Live.DimProducts_stage")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimProducts

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

dlt.apply_changes(
  target = "DimProducts",
  source = "Live.DimProducts_view",
  keys = ["product_id"],
  sequence_by = col("product_id"),
  stored_as_scd_type = 2
)