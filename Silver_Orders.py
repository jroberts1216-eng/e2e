# Databricks notebook source
# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@jroberts1216a.dfs.core.windows.net/orders")


# COMMAND ----------

df = df.withColumnRenamed("_rescued_data", "rescued_data")

# COMMAND ----------

df = df.drop("rescued_data")
df.display

# COMMAND ----------

# MAGIC %md
# MAGIC # Column Functions

# COMMAND ----------


df = df.withColumn("order_date", to_timestamp(col('order_date')))
display(df)

# COMMAND ----------


df = df.withColumn("Year", year(col("order_date")))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Window Functions

# COMMAND ----------

# Rank cost by Year

df1 = df.withColumn("flag",dense_rank().over(Window.partitionBy("Year").orderBy(col("total_amount").desc())))
display(df1)

# COMMAND ----------

df1 = df1.withColumn("rank_flag",rank().over(Window.partitionBy("Year").orderBy(col("total_amount").desc())))
display(df1)

# COMMAND ----------

df1 = df1.withColumn("row_flag",row_number().over(Window.partitionBy("Year").orderBy(col("total_amount").desc())))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Classes - OOP - Object Oriented Programming

# COMMAND ----------

class windows:

  def dense_rank(self,df):
    df_dense_rank = df.withColumn("flag",dense_rank().over(Window.partitionBy("Year").orderBy(col("total_amount").desc())))
    return df_dense_rank
  
  def rank(self,df):
    df_rank = df.withColumn("rank_flag",rank().over(Window.partitionBy("Year")))
    return df_rank
  
  def row_number(self,df):
    df_row_number = df.withColumn("row_flag",row_number().over(Window.partitionBy("Year")))
    return df_row_number
  

# COMMAND ----------

df_new = df

# COMMAND ----------

df_new.display()

# COMMAND ----------

obj = windows() 

# COMMAND ----------

df_result = obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@jroberts1216a.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.orders_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@jroberts1216a.dfs.core.windows.net/orders'