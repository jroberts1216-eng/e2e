# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic Parameters

# COMMAND ----------

dbutils.widgets.text("file_name", "")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

### Display Data Checks ###
'''
df = spark.read.format("parquet").load("abfss://source@jroberts1216a.dfs.core.windows.net/orders")
df.display() 

df = spark.read.format("parquet").load("abfss://bronze@jroberts1216a.dfs.core.windows.net/orders")
df.display() 
'''

# COMMAND ----------

### Read Stream ###
df = spark.readStream.format("cloudfiles")\
            .option("cloudFiles.format", "parquet")\
            .option("cloudFiles.schemaLocation", f"abfss://bronze@jroberts1216a.dfs.core.windows.net/checkpoint_{p_file_name}")\
            .load(f"abfss://source@jroberts1216a.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

### Write New Orders Data to Bronze ###

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation", f"abfss://bronze@jroberts1216a.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path", f"abfss://bronze@jroberts1216a.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()



# COMMAND ----------

df = spark.read.format("parquet")\
    .load(f"abfss://bronze@jroberts1216a.dfs.core.windows.net/{p_file_name}")
display(df)