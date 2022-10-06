# Databricks notebook source
import pyspark.pandas as ks
ks.set_option('compute.ops_on_diff_frames', True)
import numpy as np

from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import isnan, when, count
from pyspark.sql.types import DateType, StringType
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd


import dlt 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 1: Bronze layer - Raw data
# MAGIC 
# MAGIC Merge the daily closing prices of all companies with the S&P 500 index.

# COMMAND ----------

@dlt.table(name="capm_bronze")
def capm_bronze():
  capm_bronze_df = spark.sql("SELECT * FROM (SELECT to_date(Date, 'yyyy-MM-dd') as DateSP500, Close as SP500 FROM hive_metastore.indices_historical_data.sp_500) as idxs INNER JOIN (SELECT * FROM hive_metastore.stock_market_historical_data.us_closing_prices) as equities ON idxs.DateSP500 = equities.Date;").drop('DateSP500').drop('Date')
  return capm_bronze_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 2: Silver layer - Cleaned data
# MAGIC 
# MAGIC Forward fill the missing values.

# COMMAND ----------

@dlt.table(name="capm_silver")
def capm_silver():
  
  capm_silver_df = dlt.read('capm_bronze')
  missing_values_pd = capm_silver_df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in capm_silver_df.columns]).toPandas()
  
  # forward fill missing value
  forward_fill_window = (Window.rowsBetween(Window.unboundedPreceding, Window.currentRow))
  for t in missing_values_pd.columns:
    if missing_values_pd[t].iloc[0] > 0:
      capm_silver_df = (
          capm_silver_df
          .withColumn(f'{t}', F.last(f'{t}', ignorenulls=True).over(forward_fill_window))
      )
  return capm_silver_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 3: Gold layer - Returns data
# MAGIC 
# MAGIC Derive the daily returns of the stocks and index.

# COMMAND ----------

# This is how we constrain SP500 not to be null
goldExpectations = {
    "sp500_not_null": "SP500 IS NOT NULL" 
}

@dlt.table(name="capm_gold")
@dlt.expect_all(goldExpectations)
def capm_gold():
  returns_ks = dlt.read('capm_silver').to_koalas()
  
  # calculate the log of the daily return for each stock
  returns_ks = np.log(returns_ks / returns_ks.shift(1))
  returns_ks = returns_ks.iloc[1:, :]
  return returns_ks.to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Step 4: Gold layer - beta and CAPM data
# MAGIC 
# MAGIC Follow the formulas from the previous Notebook and calculate the beta and expected return.

# COMMAND ----------

@dlt.table(name="capm_betas_and_returns")
def betas_and_capmreturn():
  
  # 10 Year Treasury Rate as of August 4, 2022
  r_f = 0.0268
  
  # SP500 return for 2021, which we will use as a benchmark
  r_m = 0.2689
  
  returns_ks = dlt.read('capm_gold').to_koalas()

  cov_ks = returns_ks.cov() * 250
  
  companies_dct = []
  betas_schema = StructType([ \
    StructField("Company", StringType(), True), \
    StructField("Beta", DoubleType(), True), \
    StructField("Return", DoubleType(), True)
  ])
  
  # Annualized variance of SP 500 
  market_var = returns_ks['SP500'].var() * 250
  
  for t in cov_ks.columns[1:]:
    
    # Covariance of the return of each comapny with the return of SP 500
    cov_with_market = cov_ks[t].iloc[0]
    
    # Company beta
    beta = cov_with_market / market_var
    
    # CAPM formula
    capm = r_f + beta * (r_m - r_f)
    companies_dct.append({'Company': t, 'Beta': float(beta), 'Return' : float(capm)})
  return spark.createDataFrame(data = companies_dct, schema = betas_schema)

# COMMAND ----------



# COMMAND ----------


