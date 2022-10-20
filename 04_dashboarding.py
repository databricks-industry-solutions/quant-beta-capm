# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src='https://bbb-databricks-demo-assets.s3.amazonaws.com/capm_dash.png' style="float: center" width="1250px"  />

# COMMAND ----------

version_before_last = spark.sql("describe history hive_metastore.capm_dlt_output.capm_gold limit 2").select("version").collect()[1][0]
spark.sql(f"select * from hive_metastore.capm_dlt_output.capm_gold version as of {version_before_last}").display()

# COMMAND ----------


