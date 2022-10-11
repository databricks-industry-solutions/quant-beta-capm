# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC <img src='https://bbb-databricks-demo-assets.s3.amazonaws.com/capm_dash.png' style="float: center" width="1250px"  />

# COMMAND ----------

spark.sql('select * from hive_metastore.capm_dlt_output.capm_gold').to_koalas().plot.scatter(x='Beta', y='Return') 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.capm_dlt_output.capm_gold version as of 3

# COMMAND ----------


