# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Medicare Post-Acute and Hospice (PAC)
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

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "muppac" # destination table

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE mimi_ws_1.datacmsgov.muppac_geo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geo-level (_geo)

# COMMAND ----------

tablename2 = f"{tablename}_geo"
files_latest = {}
pathobj = Path(f"{path}/{tablename}")
for filepath in pathobj.glob("*"):
    dy = filepath.stem[-5:-1]
    ry = filepath.stem[2:6]
    dt = parse(f"{dy}-12-31").date()
    rt = parse(f"{ry}-12-31").date()
    if dt not in files_latest:
        files_latest[dt] = (dt, filepath, rt)
    elif files_latest[dt][2] < rt:
        files_latest[dt] = (dt, filepath, rt)
files = sorted([x for x in files_latest.values()], key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"bene_dstnct_cnt",
               "tot_espd_stay_cnt",
               "tot_srvc_days"}
double_columns = {"tot_chrg_amt",
                  "tot_alowd_amt",
                  "tot_mdcr_pymt_amt",
                  "tot_mdcr_stdzd_pymt_amt",
                  "tot_outlier_pymt_amt",
                  "bene_dual_pct",
                  "bene_rrl_pct",
                  "bene_avg_age",
                  "bene_male_pct",
                  "bene_feml_pct",
                  "bene_race_wht_pct",
                  "bene_race_black_pct",
                  "bene_race_api_pct",
                  "bene_race_hspnc_pct",
                  "bene_race_natind_pct",
                  "bene_race_unk_pct",
                  "bene_race_othr_pct",
                  "bene_avg_risk_scre",
                  "bene_avg_cc_cnt",
                  "bene_cc_af_pct",
                  "bene_cc_alzhmr_pct",
                  "bene_cc_asthma_pct",
                  "bene_cc_cncr_pct",
                  "bene_cc_chf_pct",
                  "bene_cc_ckd_pct",
                  "bene_cc_copd_pct",
                  "bene_cc_dprssn_pct",
                  "bene_cc_dbts_pct",
                  "bene_cc_hyplpdma_pct",
                  "bene_cc_hyprtnsn_pct",
                  "bene_cc_ihd_pct",
                  "bene_cc_opo_pct", 
                  "bene_cc_raoa_pct",
                  "bene_cc_sz_pct",
                  "bene_cc_strok_pct",
                  "prmry_dx_infctn_pct",
                  "prmry_dx_neobld_pct",
                  "prmry_dx_endonutrmet_pct",
                  "prmry_dx_mntbehneudis_pct",
                  "prmry_dx_nervsystm_pct",
                  "prmry_dx_entsys_pct",
                  "prmry_dx_circsystm_pct",
                  "prmry_dx_rspsystm_pct",
                  "prmry_dx_digsystm_pct",
                  "prmry_dx_sknmussystm_pct",
                  "prmry_dx_gusystm_pct",
                  "prmry_dx_prgpericong_pct",
                  "prmry_dx_sxilldef_pct",
                  "prmry_dx_injpois_pct",
                  "prmry_dx_hlthsrv_pct", 
                  "nrsng_visits_cnt",
                  "msw_visits_cnt",
                  "aide_visits_cnt",
                  "tot_nrsng_mnts",
                  "tot_pt_mnts",
                  "indvdl_pt_mnts",
                  "cncrnt_grp_pt_mnts",
                  "cotrt_pt_mnts",
                  "tot_ot_mnts",
                  "indvdl_ot_mnts",
                  "cncrnt_grp_ot_mnts",
                  "cotrt_ot_mnts",
                  "tot_slp_mnts",
                  "indvdl_slp_mnts",
                  "cncrnt_grp_slp_mnts",
                  "cotrt_slp_mnts",
                  "hospc_rhc_days_pct",
                  "tot_hh_lupa_epsds_cnt"}

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
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[\$,%]", "").cast("int"))
        elif col_new in double_columns:
            df = df.withColumn(col_new, 
                               regexp_replace(
                                regexp_replace(col(col_old), "[\$,%]", ""),
                                "N/A", lit(None)).cast("double"))
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


