# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Medicare DME files
# MAGIC
# MAGIC Five different levels of files exist:
# MAGIC
# MAGIC - referring provider-level (with a postfix, "_prvdr")
# MAGIC - geographic-level (with a postfix, "_geo")
# MAGIC - referring provider-service-level (**main**, no postfix)
# MAGIC - supplier provider - level
# MAGIC - supplier provider-service-level
# MAGIC
# MAGIC We ingest the main file, and then the rest. 
# MAGIC

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, to_date, regexp_replace
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import itertools

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "mupdme" # destination table

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.mupdme;
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.mupdme_prvdr;
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.mupdme_geo;

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.mupdme_sup;
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.mupdme_suphpr;

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referring Provider-Service-level (main)

# COMMAND ----------

files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in itertools.chain(pathobj.glob("*_prvhpr*"), pathobj.glob("*_rfrhpr*")):
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

int_columns = {"tot_suplrs", "tot_suplr_benes", 
               "tot_suplr_clms", "tot_suplr_srvcs"}
double_columns = {"avg_suplr_sbmtd_chrg", 
                  "avg_suplr_mdcr_alowd_amt",
                  "avg_suplr_mdcr_pymt_amt",
                  "avg_suplr_mdcr_stdzd_amt"}
legacy_columns = {"rfrg_prvdr_last_name": "rfrg_prvdr_last_name_org",
                  "rfrg_crdntls": "rfrg_prvdr_crdntls",
                  "rfrg_ent_cd": "rfrg_prvdr_ent_cd",
                   "betos_lvl":  "rbcs_lvl",
                    "betos_cd": "rbcs_id",
                    "betos_desc": "rbcs_desc",
                  "rfrg_prvdr_type": "rfrg_prvdr_spclty_desc",
                  "rfrg_prvdr_type_flag": "rfrg_prvdr_spclty_srce"}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    
    # mup_dme_r19_p08_v10_d13_prvhpr.csv has ill-formatted entries
    # WARNING/NOTE: we are removing those rows
    # K0056 <- this is the HCPCS code causing issue. The dataset will not contain this HCPCS code
    df = df.filter(col('Tot_Suplrs') != ' or ultralightweight wheelchair"')

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
# MAGIC
# MAGIC ## Referring Provider-level (_prvdr)

# COMMAND ----------

tablename2 = f"{tablename}_prvdr"
files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in itertools.chain(pathobj.glob("*_prvr.csv"),
                                pathobj.glob("*_Prov.csv"),
                                pathobj.glob("*_rfrr.csv")):
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

int_columns = {"tot_suplrs", 
               "tot_suplr_hcpcs_cds",
               "tot_suplr_benes", 
               "tot_suplr_clms", 
               "tot_suplr_srvcs",
               "dme_tot_suplrs",
               "dme_tot_suplr_hcpcs_cds",
               "dme_tot_suplr_benes",
               "dme_tot_suplr_clms",
               "dme_tot_suplr_srvcs",
               "pos_tot_suplrs",
               "pos_tot_suplr_hcpcs_cds",
               "pos_tot_suplr_benes",
               "pos_tot_suplr_clms",
               "pos_tot_suplr_srvcs",
               "drug_tot_suplrs",
               "drug_tot_suplr_hcpcs_cds",
               "drug_tot_suplr_benes",
               "drug_tot_suplr_clms",
               "drug_tot_suplr_srvcs",
               "bene_age_lt_65_cnt", "bene_age_65_74_cnt", 
                "bene_age_75_84_cnt", "bene_age_gt_84_cnt", 
                "bene_feml_cnt", "bene_male_cnt", 
                "bene_race_wht_cnt", "bene_race_black_cnt", 
                "bene_race_api_cnt", "bene_race_hspnc_cnt", 
                "bene_race_natind_cnt", "bene_race_othr_cnt",
                "bene_dual_cnt", "bene_ndual_cnt"
               }
double_columns = {"suplr_sbmtd_chrgs", 
                  "suplr_mdcr_alowd_amt",
                  "suplr_mdcr_pymt_amt",
                  "suplr_mdcr_stdzd_pymt_amt",
                  "dme_suplr_sbmtd_chrgs",
                  "dme_suplr_mdcr_alowd_amt",
                  "dme_suplr_mdcr_pymt_amt",
                  "dme_suplr_mdcr_stdzd_pymt_amt",
                  "pos_suplr_sbmtd_chrgs",
                  "pos_suplr_mdcr_alowd_amt",
                  "pos_suplr_mdcr_pymt_amt",
                  "pos_suplr_mdcr_stdzd_pymt_amt",
                  "drug_suplr_sbmtd_chrgs",
                  "drug_suplr_mdcr_alowd_amt",
                  "drug_suplr_mdcr_pymt_amt",
                  "drug_suplr_mdcr_stdzd_pymt_amt",
                  "bene_avg_age",
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
                  "bene_avg_risk_scre"
                  }
legacy_columns = {"rfrg_prvdr_last_name": "rfrg_prvdr_last_name_org",
                  "rfrg_prvdr_gnder": "rfrg_prvdr_gndr",
                  "pos_tot_suplr_suplrs": "pos_tot_suplrs",
                  "drug_tot_suplr_suplrs": "drug_tot_suplrs",
                  "drug_tot_suplr_sbmtd_chrgs": "drug_suplr_sbmtd_chrgs",
                  "bene_cc_asthma_pct": "bene_cc_ph_asthma_v2_pct",
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
                    "bene_cc_chf_pct": "bene_cc_ph_arthritis_v2_pct",
                    "bene_cc_stroke_pct": "bene_cc_ph_stroke_tia_v2_pct",
                    "bene_cc_alzhmr_pct": "bene_cc_bh_alz_nonalzdem_v2_pct",
                    "bene_cc_dprssn_pct": "bene_cc_bh_depress_v1_pct",
                    "bene_cc_raoa_pct": "bene_cc_ph_arthritis_v2_pct",
                    "bene_cc_sz_pct": "bene_cc_bh_schizo_othpsy_v1_pct",
                  "rfrg_prvdr_type": "rfrg_prvdr_spclty_desc",
                  "rfrg_prvdr_type_flag": "rfrg_prvdr_spclty_srce"
                  }

for item in files:
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
    
    # file schemas have changed over time...
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
for filepath in pathobj.glob("*_geo*"):
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

int_columns = {"tot_rfrg_prvdrs", 
               "tot_suplrs", 
               "tot_suplr_benes",
               "tot_suplr_clms",
               "tot_suplr_srvcs"}
double_columns = {"avg_suplr_sbmtd_chrg",
                  "avg_suplr_mdcr_alowd_amt"
                  "avg_suplr_mdcr_pymt_amt",
                  "avg_suplr_mdcr_stdzd_amt"}
legacy_columns = {"betos_lvl": "rbcs_lvl",
                    "betos_cd": "rbcs_id",
                    "betos_desc": "rbcs_desc"}

for item in files:
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    # mup_dme_r19_p08_v10_d13_prvhpr.csv has ill-formatted entries
    # WARNING/NOTE: we are removing those rows
    # K0056 <- this is the HCPCS code causing issue. The dataset will not contain this HCPCS code
    df = df.filter(col('Tot_Rfrg_Prvdrs') != ' or ultralightweight wheelchair"')

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
    
    ifd_str = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("replaceWhere", f"_input_file_date = '{ifd_str}'")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename2}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supplier Provider x Service

# COMMAND ----------

tablename2 = f"{tablename}_suphpr"
files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in pathobj.glob("*_suphpr*"):
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


int_columns = {"tot_suplrs", "tot_suplr_benes", 
               "tot_suplr_clms", "tot_suplr_srvcs"}
double_columns = {"avg_suplr_sbmtd_chrg", 
                  "avg_suplr_mdcr_alowd_amt",
                  "avg_suplr_mdcr_pymt_amt",
                  "avg_suplr_mdcr_stdzd_amt"}
legacy_columns = {"betos_lvl":  "rbcs_lvl",
                    "betos_cd": "rbcs_id",
                    "betos_desc": "rbcs_desc"}

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    
    # mup_dme_r19_p08_v10_d13_prvhpr.csv has ill-formatted entries
    # WARNING/NOTE: we are removing those rows
    # K0056 <- this is the HCPCS code causing issue. The dataset will not contain this HCPCS code
    df = df.filter(col('Tot_Suplr_Benes') != ' or ultralightweight wheelchair"')

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
        .saveAsTable(f"{catalog}.{schema}.{tablename2}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Supplier-level

# COMMAND ----------

tablename2 = f"{tablename}_sup"
files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in pathobj.glob("*_supr*"):
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

int_columns = {"tot_suplrs", 
               "tot_suplr_hcpcs_cds",
               "tot_suplr_benes", 
               "tot_suplr_clms", 
               "tot_suplr_srvcs",
               "dme_tot_suplrs",
               "dme_tot_suplr_hcpcs_cds",
               "dme_tot_suplr_benes",
               "dme_tot_suplr_clms",
               "dme_tot_suplr_srvcs",
               "pos_tot_suplrs",
               "pos_tot_suplr_hcpcs_cds",
               "pos_tot_suplr_benes",
               "pos_tot_suplr_clms",
               "pos_tot_suplr_srvcs",
               "drug_tot_suplrs",
               "drug_tot_suplr_hcpcs_cds",
               "drug_tot_suplr_benes",
               "drug_tot_suplr_clms",
               "drug_tot_suplr_srvcs",
               "bene_age_lt_65_cnt", "bene_age_65_74_cnt", 
                "bene_age_75_84_cnt", "bene_age_gt_84_cnt", 
                "bene_feml_cnt", "bene_male_cnt", 
                "bene_race_wht_cnt", "bene_race_black_cnt", 
                "bene_race_api_cnt", "bene_race_hspnc_cnt", 
                "bene_race_natind_cnt", "bene_race_othr_cnt",
                "bene_dual_cnt", "bene_ndual_cnt"
               }
double_columns = {"suplr_sbmtd_chrgs", 
                  "suplr_mdcr_alowd_amt",
                  "suplr_mdcr_pymt_amt",
                  "suplr_mdcr_stdzd_pymt_amt",
                  "dme_suplr_sbmtd_chrgs",
                  "dme_suplr_mdcr_alowd_amt",
                  "dme_suplr_mdcr_pymt_amt",
                  "dme_suplr_mdcr_stdzd_pymt_amt",
                  "pos_suplr_sbmtd_chrgs",
                  "pos_suplr_mdcr_alowd_amt",
                  "pos_suplr_mdcr_pymt_amt",
                  "pos_suplr_mdcr_stdzd_pymt_amt",
                  "drug_suplr_sbmtd_chrgs",
                  "drug_suplr_mdcr_alowd_amt",
                  "drug_suplr_mdcr_pymt_amt",
                  "drug_suplr_mdcr_stdzd_pymt_amt",
                  "bene_avg_age",
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
                  "bene_avg_risk_scre"
                  }
legacy_columns = {"rfrg_prvdr_last_name": "rfrg_prvdr_last_name_org",
                  "rfrg_prvdr_gnder": "rfrg_prvdr_gndr",
                  "pos_tot_suplr_suplrs": "pos_tot_suplrs",
                  "drug_tot_suplr_suplrs": "drug_tot_suplrs",
                  "drug_tot_suplr_sbmtd_chrgs": "drug_suplr_sbmtd_chrgs",
                  "bene_cc_asthma_pct": "bene_cc_ph_asthma_v2_pct",
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
                    "bene_cc_chf_pct": "bene_cc_ph_arthritis_v2_pct",
                    "bene_cc_stroke_pct": "bene_cc_ph_stroke_tia_v2_pct",
                    "bene_cc_alzhmr_pct": "bene_cc_bh_alz_nonalzdem_v2_pct",
                    "bene_cc_dprssn_pct": "bene_cc_bh_depress_v1_pct",
                    "bene_cc_raoa_pct": "bene_cc_ph_arthritis_v2_pct",
                    "bene_cc_sz_pct": "bene_cc_bh_schizo_othpsy_v1_pct",
                  "rfrg_prvdr_type": "rfrg_prvdr_spclty_desc",
                  "rfrg_prvdr_type_flag": "rfrg_prvdr_spclty_srce"
                  }

for item in files:
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
    
    # file schemas have changed over time...
    ifd_str = item[0].strftime('%Y-%m-%d')
    (df.write
        .format('delta')
        .mode("overwrite")
        .option("replaceWhere", f"_input_file_date = '{ifd_str}'")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename2}"))
    

# COMMAND ----------


