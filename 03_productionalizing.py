# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Productionalizing the whole solution
# MAGIC 
# MAGIC In order to create a robust data pipeline (that can handle the many files and many processing steps) we will use Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables).

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://github.com/databricks-industry-solutions/quant-beta-capm/raw/main/DLT_DAG.png' style="float: center" width="1250px"  />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 1. Delta Live Tables
# MAGIC 
# MAGIC As already mentioned, Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) (DLT) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake. DLT helps data engineering teams simplify ETL development and management with declarative pipeline development, automatic data testing, and deep visibility for monitoring and recovery.
# MAGIC 
# MAGIC A few of the **capabilities** DLT brings us are:
# MAGIC - **Automatic testing**: DLT helps to ensure accurate and useful BI, data science and machine learning with high-quality data for downstream users. Prevent bad data from flowing into tables through validation and integrity checks and avoid data quality errors with predefined error policies. As we can see from the image above, in the last step of the pipeline (```capm_betas_and_returns```, where we calculate the equity beta and the expected return) _all of the records (a little bit more than 4,000) were successfully processed and none were dropped_.
# MAGIC - **Deep visibility for monitoring and easy recovery**: Gain deep visibility into pipeline operations with tools to visually track operational stats and data lineage. We can see all four steps (outlined in the ```CAPM - 2. Equity Beta + Return Calculation``` notebook), their execution, and potential errors.
# MAGIC - **Simplified management and governance**: DLT's UI gives us very easy management of the whole data pipeline, including Dev to Prod deployment, permissions management, scheduling executions, and running.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## 2.  Time travelling
# MAGIC 
# MAGIC Delta automatically versions the big data that companies store in their data lakes so they can access any historical version of that data. This temporal data management simplifies data pipelines by making it easy to audit, roll back data in case of accidental bad writes or deletes, and reproduce reports.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM hive_metastore.stock_market_historical_data.us_closing_100 VERSION AS OF 0

# COMMAND ----------


