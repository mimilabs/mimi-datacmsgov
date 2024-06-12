# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Medicare Part D Prescribers files
# MAGIC
# MAGIC Three different levels of files exist:
# MAGIC
# MAGIC - provider-level (with a postfix, "_prvdr")
# MAGIC - geographic-level (with a postfix, "_geo")
# MAGIC - provider-service-level (**main**, no postfix)
# MAGIC
# MAGIC We ingest the main file, and then the rest. 

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, to_date, regexp_replace
from datetime import datetime
from dateutil.parser import parse
import pandas as pd

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "mupdpr" # destination table

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider-Service-level (main)

# COMMAND ----------

files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in pathobj.glob("*_NPIBN*"):
    dy = '20' + re.search("\_d[y]*(\d+)\_", filepath.stem.lower()).group(1)
    ry = '20' + re.search("\_r[y]*(\d+)\_", filepath.stem.lower()).group(1)
    dt = parse(f"{dy}-12-31").date()
    rt = parse(f"{ry}-12-31").date()
    if dt not in files_latest:
        files_latest[dt] = (dt, filepath, rt)
    elif files_latest[dt][2] < rt:
        files_latest[dt] = (dt, filepath, rt)
files = sorted([x for x in files_latest.values()], key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"tot_clms", "tot_day_suply",  "tot_benes",
                "ge65_tot_clms",  "ge65_tot_day_suply", "ge65_tot_benes"}
double_columns = {"tot_30day_fills", "tot_drug_cst",
                  "ge65_tot_30day_fills", "ge65_tot_drug_cst"}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):
        header.append(col_new)
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("double"))
        else:
            df = df.withColumn(col_new, col(col_old))
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0]))
          .withColumn("_source_file_name", lit(item[1].name)))
    
    ifd_str = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("replaceWhere", f"_input_file_date = '{ifd_str}'")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Provider-level (_prvdr)

# COMMAND ----------

tablename2 = f"{tablename}_prvdr"
files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in pathobj.glob("*_NPI.csv"):
    dy = '20' + re.search("\_d[y]*(\d+)\_", filepath.stem.lower()).group(1)
    ry = '20' + re.search("\_r[y]*(\d+)\_", filepath.stem.lower()).group(1)
    dt = parse(f"{dy}-12-31").date()
    rt = parse(f"{ry}-12-31").date()
    if dt not in files_latest:
        files_latest[dt] = (dt, filepath, rt)
    elif files_latest[dt][2] < rt:
        files_latest[dt] = (dt, filepath, rt)
files = sorted([x for x in files_latest.values()], key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"tot_clms", "tot_day_suply",  "tot_benes",
                "ge65_tot_clms",  "ge65_tot_day_suply", "ge65_tot_benes",
                "brnd_tot_clms", "gnrc_tot_clms", "othr_tot_clms", 
                "mapd_tot_clms", "pdp_tot_clms", "lis_tot_clms", "nonlis_tot_clms",
                "opioid_tot_clms", "opioid_tot_suply", "opioid_tot_benes", 
                "opioid_la_tot_clms", "opioid_la_tot_suply", "opioid_la_tot_benes",
                "antbtc_tot_clms", "antbtc_tot_benes",
                "antpsyct_ge65_tot_clms", "antpsyct_ge65_tot_benes",
                "bene_age_lt_65_cnt", "bene_age_65_74_cnt", 
                "bene_age_75_84_cnt", "bene_age_gt_84_cnt", 
                "bene_feml_cnt", "bene_male_cnt", 
                "bene_race_wht_cnt", "bene_race_black_cnt", 
                "bene_race_api_cnt", "bene_race_hspnc_cnt", 
                "bene_race_natind_cnt", "bene_race_othr_cnt",
                "bene_dual_cnt", "bene_ndual_cnt"}
double_columns = {"tot_30day_fills", "tot_drug_cst", 
                  "ge65_tot_30day_fills", "ge65_tot_drug_cst",
                  "brnd_tot_drug_cst", "gnrc_tot_drug_cst", "othr_tot_drug_cst",
                  "mapd_tot_drug_cst", "pdp_tot_drug_cst", "lis_drug_cst", "nonlis_tot_cst",
                  "opioid_tot_drug_cst", "opioid_prscrbr_rate", 
                  "opioid_la_tot_drug_cst", "opioid_la_prscrbr_rate", 
                  "antbtc_tot_drug_cst", 
                  "antpsyct_ge65_tot_drug_cst",
                  "bene_avg_age", "bene_avg_risk_scre"}

for item in files:
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):
        header.append(col_new)
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("double"))
        else:
            df = df.withColumn(col_new, col(col_old))
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0]))
          .withColumn("_source_file_name", lit(item[1].name)))
    
    ifd_str = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("replaceWhere", f"_input_file_date = '{ifd_str}'")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename2}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geo-level (_geo)

# COMMAND ----------

tablename2 = f"{tablename}_geo"
files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in pathobj.glob("*_Geo*"):
    dy = '20' + re.search("\_d[y]*(\d+)\_", filepath.stem.lower()).group(1)
    ry = '20' + re.search("\_r[y]*(\d+)\_", filepath.stem.lower()).group(1)
    dt = parse(f"{dy}-12-31").date()
    rt = parse(f"{ry}-12-31").date()
    if dt not in files_latest:
        files_latest[dt] = (dt, filepath, rt)
    elif files_latest[dt][2] < rt:
        files_latest[dt] = (dt, filepath, rt)
files = sorted([x for x in files_latest.values()], key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"tot_prscrbrs",
                "tot_clms", 
                "tot_benes",
                "ge65_tot_clms", 
                "ge65_tot_benes"}
double_columns = {"tot_30day_fills", 
                  "tot_drug_cst", 
                  "ge65_tot_30day_fills", 
                  "ge65_tot_drug_cst",
                  "lis_bene_cst_shr",
                  "nonlis_bene_cst_shr"}

for item in files:
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):
        header.append(col_new)
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("double"))
        else:
            df = df.withColumn(col_new, col(col_old))
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0]))
          .withColumn("_source_file_name", lit(item[1].name)))
    
    ifd_str = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("replaceWhere", f"_input_file_date = '{ifd_str}'")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename2}"))

# COMMAND ----------


