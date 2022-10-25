# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Equity Beta Calculation and CAPM
# MAGIC 
# MAGIC In this solution, we will calculate the beta (a measurement of the volatility, or systematic risk, of equity, as it compares to the broader market, such as S&500 or NYSE) of more than 4,000 US-listed companies and then use it to derive the expected return on investment (again for each company) using the Capital Asset Pricing Model (CAPM).
# MAGIC 
# MAGIC We will use daily data (closing price) starting January 1st, 2010 until the end of July 2022 (more than 12 years). The data is shown in ```Cmd 7``` cell below.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As shown in the formula below (```Formula 1```), deriving the company's beta requires calculating the covariance between the returns of the company and a market benchmark, for which we will use the S&P 500 index, and also the variance of the market, both of which are very computationally intensive tasks, especially when have 12+ years data for more than 4,000 companies.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ```Formula 1``` Equity beta calculation
# MAGIC 
# MAGIC ## \\( \beta{_i} = \frac{\sigma{_i},{_m}}{\sigma{_m}} \\)
# MAGIC 
# MAGIC _where_: <br />
# MAGIC - \\( \beta{_i} \\) is the beta of a company,
# MAGIC - \\( \sigma{_i},{_m} \\) is the covariance between the return a stock and the market benchmark, and
# MAGIC - \\( \sigma{_m} \\) is the variance of the market.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## ```Formula 2``` Capital Asset Pricing Model (CAPM)
# MAGIC 
# MAGIC ### \\(E(R{_e}) = R{_f} + \beta{_i} * (E(R{_m}) - R{_f}) \\)
# MAGIC 
# MAGIC _where_: <br />
# MAGIC - \\(E(R{_e}) \\) is the expected return of investment,
# MAGIC - \\(R{_f} \\) is the risk-free rate,
# MAGIC - \\(\beta{_i} \\) is the beta of the equity, and 
# MAGIC - \\(E(R{_m}) - R{_f} \\) represents the market risk premium.
# MAGIC 
# MAGIC For the risk-free rate we will use the 10 year Treasury rate as of Aug 4th, 2022 (at 0.0268) and for the market return we will use the 2021 return of the S&P500.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Why Databricks Lakehouse for Equity Beta calculation and CAPM?
# MAGIC  
# MAGIC 
# MAGIC 1. **Data Volume and Variety**: We have more than 4,000 files (12+ years of daily OHLC + Volume for almost every US-listed company). Manually managing and optimizing all these files can be tedious. To improve query speed, [Delta Lake on Databricks](https://docs.databricks.com/delta/optimizations/file-mgmt.html) supports the ability to optimize the layout of data stored in cloud storage. Delta Lake on Databricks supports two layout algorithms: bin-packing and Z-Ordering.
# MAGIC 2. **Enforcing data quality and schema detection/enforcement**
# MAGIC    * Some of the data files have missing values (as imported from the data vendor - see ```Cmd 9``` below) and we need to make sure we do not have missing records.
# MAGIC    * Another problem with having many files and injecting data from external vendors is that we need to make sure any changes (or mismatch) of the schema of the files will not break any downstream processing of the data. [Delta Lake's Schema Enforcement](https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html) uses schema validation on write, which means that all new writes to a table are checked for compatibility with the target table’s schema at write time. If the schema is not compatible, Delta Lake cancels the transaction altogether (no data is written), and raises an exception to let the user know about the mismatch.
# MAGIC 3. **Data Lineage**: Having so many files and processing steps, we need to make sure we can trace back data processes or missing files. This is where [Unity Catalog](https://www.databricks.com/product/unity-catalog) helps.  Unity Catalog's [Data Lineage](https://www.databricks.com/blog/2022/06/08/announcing-the-availability-of-data-lineage-with-unity-catalog.html) automates run-time lineage, supports all workloads, works on column level, and covers notebooks, scripts, third-party tools, and dashboards.
# MAGIC 4. **Time travel**: What if we want to observe the version of the raw data used to calculate the stock betas and CAPM for a specific point of time. Delta automatically versions the big data that companies store in their data lakes so they can access any historical version of that data.
# MAGIC 5. **Scale**: The burst capacity of [Databricks Runtime](https://docs.databricks.com/runtime/mlruntime.html), powered by [Photon](https://www.databricks.com/product/photon), can run these very computationally intensive calculations extremely quickly and in a cost-efficient way,
# MAGIC 6. **Data Pipeline**: Orchestrating the whole process through a robust data pipeline that can show the data quality, enforce [constraints](https://docs.databricks.com/delta/delta-constraints.html), schedule execution, and monitor execution is important for productionalizing the whole solution. Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) (DLT) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake. DLT helps data engineering teams simplify ETL development and management with declarative pipeline development, automatic data testing, and deep visibility for monitoring and recovery.
# MAGIC 7. **Process new coming data**: Databricks' [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup. If we want to have an accurate measurement of \\(\beta{_i} \\) and \\(E(R{_e}) \\) requires calculating them often.
# MAGIC 8. **Visualize**: After calculating the beta and expected return for every company we will visualize all company information in a Databricks SQL Dashboard. [Databricks SQL](https://www.databricks.com/product/databricks-sql) (DB SQL) is a serverless data warehouse on the Databricks Lakehouse Platform that lets you run all your SQL and BI applications at scale with up to 12x better price/performance, a unified governance model, open formats and APIs, and your tools of choice – no lock-in.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Architecture
# MAGIC 
# MAGIC This is the architecture of the solution we will build.
# MAGIC <img src='https://github.com/databricks-industry-solutions/quant-beta-capm/raw/main/capm_arch.png' style="float: center" width="1400px"  />

# COMMAND ----------

# DBTITLE 1,Create tables to read source data
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.stock_market_historical_data;
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.indices_historical_data;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS hive_metastore.stock_market_historical_data.us_closing_100;
# MAGIC 
# MAGIC CREATE TABLE hive_metastore.stock_market_historical_data.us_closing_100
# MAGIC LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/us_closing_100/';
# MAGIC 
# MAGIC DROP TABLE IF EXISTS hive_metastore.indices_historical_data.sp_500;
# MAGIC 
# MAGIC CREATE TABLE hive_metastore.indices_historical_data.sp_500
# MAGIC LOCATION 's3a://db-gtm-industry-solutions/data/fsi/capm/sp_500/';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.stock_market_historical_data.us_closing_100

# COMMAND ----------

closing_prices_df = spark.sql('select * from hive_metastore.stock_market_historical_data.us_closing_100')

# COMMAND ----------

display(closing_prices_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this Notebook may be subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
