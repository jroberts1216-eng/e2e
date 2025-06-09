# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading From Source

# COMMAND ----------

df = spark.sql("select * from databricks_cata.silver.customer_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Duplicates on Primary Key (customer_id)

# COMMAND ----------

df = df.dropDuplicates(subset=['customer_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Divding New vs. Old Records

# COMMAND ----------

if init_load_flag == 0:
### DimCustomers Exists ###
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date 
                       from databricks_cata.gold.DimCustomers''')
    
else:
### Just Return Schema - No Columns ###
    df_old = spark.sql('''select 0  as DimCustomerKey, 0 as customer_id, 0 as create_date, 0 as update_date 
                       from databricks_cata.silver.customer_silver where 1=0''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming Columns of df_old

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", "old_DimCustomerKey")\
                .withColumnRenamed("customer_id", "old_customer_id")\
                .withColumnRenamed("create_date", "old_create_date")\
                .withColumnRenamed("update_date", "old_update_date")

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Applying Join with Old Records

# COMMAND ----------

df_join = df.join(df_old, df["customer_id"] == df_old["old_customer_id"], how='left')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seperating New vs. Old Records

# COMMAND ----------

df_new = df_join.filter(df_join["old_DimCustomerKey"].isNull())

# COMMAND ----------

df_old = df_join.filter(df_join["old_DimCustomerKey"].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing df_old

# COMMAND ----------

# Dropping Columns That Are Not Required

df_old = df_old.drop("old_customer_id", "old_update_date")

# Renaming old_DimCustomerKet to DimCustomerKey

df_old = df_old.withColumnRenamed("old_DimCustomerKey", "DimCustomerKey")

# Renaming old_create_date Column to create_date

df_old = df_old.withColumnRenamed("old_create_date", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))

# Recreating update_date Column

df_old = df_old.withColumn("update_date", current_timestamp())


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing df_new

# COMMAND ----------

#Dropping Columns Which Are Not Required

df_new = df_new.drop("old_DimCustomerKey", "old_customer_id", "old_create_date", "old_update_date")

#Recreating update_date and current_date

df_new = df_new.withColumn("create_date", current_timestamp())
df_new = df_new.withColumn("update_date", current_timestamp())


# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assign Surrogate Key From 1

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id()+lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Max Surrogate Key and Converting df_maxsur to max_surrogate_key variable

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0

else:
    df_maxsur = spark.sql("select max(DimCustomerKey) as max_surrogate_key from databricks_cata.gold.DimCustomers")
    # Converting df_maxsur to max_surrogate_key variable
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']


# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key)+col("DimCustomerKey"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union of df_old and df_new

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Upsert on Gold Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------


if (spark.catalog.tableExists("databricks_cata.gold.DimCustomers")):
    
    dlt_obj = DeltaTable.forPath(spark, "abfss://gold@jroberts1216a.dfs.core.windows.net/DimCustomers")


    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_final.write.format("delta").mode("overwrite")\
    .option("path", "abfss://gold@jroberts1216a.dfs.core.windows.net/DimCustomers")\
    .saveAsTable("databricks_cata.gold.DimCustomers")
