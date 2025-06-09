# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Products

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df = spark.read.format("parquet")\
  .load("abfss://bronze@jroberts1216a.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_id, price, databricks_cata.bronze.discount_func(price) as discounted_price
# MAGIC FROM products

# COMMAND ----------

df = df.withColumn("discounted_price", expr("databricks_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION databricks_cata.bronze.upper_func(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS 
# MAGIC $$ 
# MAGIC   return p_brand.upper('brand')
# MAGIC $$

# COMMAND ----------

df.write.format("delta")\
  .mode("overwrite")\
.option("path","abfss://silver@jroberts1216a.dfs.core.windows.net/products")\
.save() 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.products_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@jroberts1216a.dfs.core.windows.net/products'