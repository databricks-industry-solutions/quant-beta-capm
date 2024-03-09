<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-10.4ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/10.4ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# Equity Beta Calculation and CAPM

In this solution, we will calculate the beta (a measurement of the volatility, or systematic risk, of equity, as it compares to the broader market, such as S&500 or NYSE) of more than 4,000 US-listed companies and then use it to derive the expected return on investment (again for each company) using the Capital Asset Pricing Model (CAPM).

We will use daily data (closing price) starting January 1st, 2010 until the end of July 2022 (more than 12 years)

## Why Databricks Lakehouse for Equity Beta calculation and CAPM?
 

1. **Data Volume and Variety**: We have more than 4,000 files (12+ years of daily OHLC + Volume for almost every US-listed company). Manually managing and optimizing all these files can be tedious. To improve query speed, [Delta Lake on Databricks](https://docs.databricks.com/delta/optimizations/file-mgmt.html) supports the ability to optimize the layout of data stored in cloud storage. Delta Lake on Databricks supports two layout algorithms: bin-packing and Z-Ordering.
2. **Enforcing data quality and schema detection/enforcement**
   * Some of the data files have missing values (as imported from the data vendor - see ```Cmd 9``` below) and we need to make sure we do not have missing records.
   * Another problem with having many files and injecting data from external vendors is that we need to make sure any changes (or mismatch) of the schema of the files will not break any downstream processing of the data. [Delta Lake's Schema Enforcement](https://www.databricks.com/blog/2019/09/24/diving-into-delta-lake-schema-enforcement-evolution.html) uses schema validation on write, which means that all new writes to a table are checked for compatibility with the target table’s schema at write time. If the schema is not compatible, Delta Lake cancels the transaction altogether (no data is written), and raises an exception to let the user know about the mismatch.
3. **Data Lineage**: Having so many files and processing steps, we need to make sure we can trace back data processes or missing files. This is where [Unity Catalog](https://www.databricks.com/product/unity-catalog) helps.  Unity Catalog's [Data Lineage](https://www.databricks.com/blog/2022/06/08/announcing-the-availability-of-data-lineage-with-unity-catalog.html) automates run-time lineage, supports all workloads, works on column level, and covers notebooks, scripts, third-party tools, and dashboards.
4. **Time travel**: What if we want to observe the version of the raw data used to calculate the stock betas and CAPM for a specific point of time. Delta automatically versions the big data that companies store in their data lakes so they can access any historical version of that data. This temporal data management simplifies data pipelines by making it easy to audit, roll back data in case of accidental bad writes or deletes, and reproduce reports.
5. **Scale**: The burst capacity of [Databricks Runtime](https://docs.databricks.com/runtime/mlruntime.html), powered by [Photon](https://www.databricks.com/product/photon), can run these very computationally intensive calculations extremely quickly and in a cost-efficient way,
6. **Data Pipeline**: Orchestrating the whole process through a robust data pipeline that can show the data quality, enforce [constraints](https://docs.databricks.com/delta/delta-constraints.html), schedule execution, and monitor execution is important for productionalizing the whole solution. Databricks [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) (DLT) makes it easy to build and manage reliable data pipelines that deliver high-quality data on Delta Lake. DLT helps data engineering teams simplify ETL development and management with declarative pipeline development, automatic data testing, and deep visibility for monitoring and recovery.
7. **Process new coming data**: Databricks' [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup. If we want to have an accurate measurement of equity beta and expected return on equity requires calculating them often.
8. **Visualise**: After calculating the beta and expected return for every company we will visualize all company information in a Databricks SQL Dashboard. [Databricks SQL](https://www.databricks.com/product/databricks-sql) (DB SQL) is a serverless data warehouse on the Databricks Lakehouse Platform that lets you run all your SQL and BI applications at scale with up to 12x better price/performance, a unified governance model, open formats and APIs, and your tools of choice – no lock-in.

___

boris.banushev@databricks.com

___


<img src='https://github.com/databricks-industry-solutions/quant-beta-capm/raw/main/capm_arch.png' />

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |

To run this accelerator, clone this repo into a Databricks workspace. Attach the RUNME notebook to any cluster running a DBR 11.0 or later runtime, and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. Execute the multi-step-job to see how the pipeline runs.

The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.

