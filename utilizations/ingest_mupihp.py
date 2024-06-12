# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Medicare Inpatient files
# MAGIC
# MAGIC Three different levels of files exist:
# MAGIC
# MAGIC - provider-level (with a postfix, "_prvdr")
# MAGIC - geographic-level (with a postfix, "_geo")
# MAGIC - provider-service-level (**main**)
# MAGIC

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, to_date, regexp_replace, lpad
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import itertools

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "mupihp" # destination table

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.mupihp;
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.mupihp_prvdr;
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.mupihp_geo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider-Service-level (main)

# COMMAND ----------

files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in itertools.chain(pathobj.glob("*_PRVSVC*"), pathobj.glob("*_PrvSvc*")):
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

int_columns = {"tot_dschrgs"}
double_columns = {"avg_submtd_cvrd_chrg",
                  "avg_tot_pymt_amt",
                  "avg_mdcr_pymt_amt"}
legacy_columns = {}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):

        col_new = legacy_columns.get(col_new_, col_new_)
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
    
    # These columns are added later
    # - rfrg_prvdr_last_name_org (used to be rfrg_prvdr_last_name)
    # - rfrg_prvdr_ruca_cat
    # - rfrg_prvdr_type_cd
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    ifd_str = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("replaceWhere", f"_input_file_date = '{ifd_str}'")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider-level (_prvdr)

# COMMAND ----------

tablename2 = f"{tablename}_prvdr"
files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in itertools.chain(pathobj.glob("*_PRV.*"), pathobj.glob("*_Prv.*")):
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

int_columns = {"tot_dschrgs",
               "tot_benes",
               "tot_cvrd_days",
               "tot_days",
               "bene_age_lt_65_cnt",
               "bene_age_65_74_cnt",
               "bene_age_75_84_cnt",
               "bene_age_gt_84_cnt",
               "bene_feml_cnt",
               "bene_male_cnt",
               "bene_race_wht_cnt",
               "bene_race_black_cnt",
               "bene_race_api_cnt",
               "bene_race_hspnc_cnt",
               "bene_race_natind_cnt",
               "bene_race_othr_cnt",
               "bene_dual_cnt",
               "bene_ndual_cnt"}
double_columns = {"bene_avg_age",
                  "tot_submtd_cvrd_chrg",
                  "tot_pymt_amt",
                  "tot_mdcr_pymt_amt",
                  "avg_tot_sbmtd_chrgs",
                  "avg_mdcr_alowd_amt",
                  "avg_mdcr_pymt_amt",
                  "avg_mdcr_outlier_amt",
                  "bene_cc_bh_adhd_othcd_v1_pct",
                    "bene_cc_bh_alcohol_drug_v1_pct",
                    "bene_cc_bh_tobacco_v1_pct",
                    "bene_cc_bh_alz_nonalzdem_v2_pct",
                    "bene_cc_bh_anxiety_v1_pct",
                    "bene_cc_bh_bipolar_v1_pct",
                    "bene_cc_bh_mood_v2_pct",
                    "bene_cc_bh_depress_v1_pct",
                    "bene_cc_bh_pd_v1_pct",
                    "bene_cc_bh_ptsd_v1_pct",
                    "bene_cc_bh_schizo_othpsy_v1_pct",
                    "bene_cc_ph_asthma_v2_pct",
                    "bene_cc_ph_afib_v2_pct",
                    "bene_cc_ph_cancer6_v2_pct",
                    "bene_cc_ph_ckd_v2_pct",
                    "bene_cc_ph_copd_v2_pct",
                    "bene_cc_ph_diabetes_v2_pct",
                    "bene_cc_ph_hf_nonihd_v2_pct",
                    "bene_cc_ph_hyperlipidemia_v2_pct",
                    "bene_cc_ph_hypertension_v2_pct",
                    "bene_cc_ph_ischemicheart_v2_pct",
                    "bene_cc_ph_osteoporosis_v2_pct",
                    "bene_cc_ph_parkinson_v2_pct",
                    "bene_cc_ph_arthritis_v2_pct",
                    "bene_cc_ph_stroke_tia_v2_pct",
                  "bene_avg_risk_scre"}
legacy_columns = {"bene_cc_asthma_pct": "bene_cc_ph_asthma_v2_pct",
                    "bene_cc_af_pct": "bene_cc_ph_afib_v2_pct",
                    "bene_cc_cncr_pct": "bene_cc_ph_cancer6_v2_pct",
                    "bene_cc_ckd_pct": "bene_cc_ph_ckd_v2_pct",
                    "bene_cc_copd_pct": "bene_cc_ph_copd_v2_pct",
                    "bene_cc_dbts_pct": "bene_cc_ph_diabetes_v2_pct",
                    "bene_cc_chf_pct": "bene_cc_ph_hf_nonihd_v2_pct",
                    "bene_cc_hyplpdma_pct": "bene_cc_ph_hyperlipidemia_v2_pct",
                    "bene_cc_hyprtnsn_pct":"bene_cc_ph_hypertension_v2_pct",
                    "bene_cc_ihd_pct": "bene_cc_ph_ischemicheart_v2_pct",
                    "bene_cc_opo_pct": "bene_cc_ph_osteoporosis_v2_pct",
                    "bene_cc_strok_pct": "bene_cc_ph_stroke_tia_v2_pct",
                    "bene_cc_alzhmr_pct": "bene_cc_bh_alz_nonalzdem_v2_pct",
                    "bene_cc_dprssn_pct": "bene_cc_bh_depress_v1_pct",
                    "bene_cc_raoa_pct": "bene_cc_ph_arthritis_v2_pct",
                    "bene_cc_sz_pct": "bene_cc_bh_schizo_othpsy_v1_pct"}


for item in files:
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):

        col_new = legacy_columns.get(col_new_, col_new_)
        
        header.append(col_new)
        # Somehow, the CCN needs to be zero-padded for these files
        if col_new == "rndrng_prvdr_ccn":
            df = df.withColumn(col_new, lpad(col(col_old), 6, "0"))
        elif col_new in int_columns:
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
for filepath in itertools.chain(pathobj.glob("*_GEO.*"), pathobj.glob("*_Geo.*")):
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

int_columns = {"tot_dschrgs"}
double_columns = {"avg_submtd_cvrd_chrg",
                  "avg_tot_pymt_amt",
                  "avg_mdcr_pymt_amt"}

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


