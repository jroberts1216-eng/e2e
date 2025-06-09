# Databricks notebook source
df = spark.read.table("databricks_cata.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .save("abfss://silver@jroberts1216a.dfs.core.windows.net/regions")


# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@jroberts1216a.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@jroberts1216a.dfs.core.windows.net/regions'