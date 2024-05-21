# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Medicare Geographic Variation Files
# MAGIC

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.functions import col, lit, to_date, when
from datetime import datetime
from dateutil.parser import parse
import pandas as pd

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "geovariation" # destination table

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"

# COMMAND ----------

files = []
for filepath in Path(f"{path}/{tablename}").glob("*.csv"):
    dt = parse(filepath.stem[-4:]+"-12-31").date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

double_patterns = ["_amt",
                   "_pcc", 
                   "_pymt",
                    "_per_1000_benes",
                    "_pct",
                    "_per_user",
                    "_pc",
                    "_pu",
                    "_avg_age", 
                    "_rate",
                    "_scre"]
int_patterns = ["_cnt", "year"]
str_patterns = ["bene_geo_lvl", "bene_geo_desc", "bene_geo_cd", "bene_age_lvl"]
legacy_columns = {"benes_ever_ma_cnt": "benes_ma_cnt",
                "ever_ma_prtcptn_rate": "ma_prtcptn_rate",
                "asc_events_per_1000_benes": "asc_evnts_per_1000_benes",
                "hh_cvrd_stays_per_1000_benes": "hh_episodes_per_1000_benes"}

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

for item in files:
    # each file is relatively small enough (~600MB)
    # but the size is increasing every month
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):

        col_new = legacy_columns.get(col_new_, col_new_)
        
        header.append(col_new)
        if col_new in str_patterns:
            df = df.withColumn(col_new, col(col_old))
            continue
        is_double_type = False
        for double_pattern in double_patterns:
            if col_new[-len(double_pattern):] == double_pattern:
                df = df.withColumn(col_new,
                                   (when(col(col_old) == '*', None)
                                .when(col(col_old) == 'NA', None)
                            .otherwise(col(col_old).cast("double")))) 
                is_double_type = True
                break
        if not is_double_type:
            df = df.withColumn(col_new, 
                                   (when(col(col_old) == '*', None)
                                .when(col(col_old) == 'NA', None)
                            .otherwise(col(col_old).cast("int")))) 
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.datacmsgov.geovariation IS '# Medicare Geographic Variation - by National, State & County
# MAGIC
# MAGIC Information on geographic differences in the use and quality of health care services for the Original Medicare (fee-for-service) population.
# MAGIC
# MAGIC This dataset is from [https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-geographic-comparisons/medicare-geographic-variation-by-national-state-county](https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-geographic-comparisons/medicare-geographic-variation-by-national-state-county).
# MAGIC
# MAGIC According to the site:
# MAGIC
# MAGIC The Medicare Geographic Variation by National, State & County dataset provides information on the geographic differences in the use and quality of health care services for the Original Medicare population. This dataset contains demographic, spending, use, and quality indicators at the state level (including the District of Columbia, Puerto Rico, and the Virgin Islands) and the county level.
# MAGIC
# MAGIC Spending is standardized to remove geographic differences in payment rates for individual services as a source of variation. In general, total standardized per capita costs are less than actual per capita costs because the extra payments Medicare made to hospitals were removed, such as payments for medical education (both direct and indirect) and payments to hospitals that serve a disproportionate share of low-income patients. Standardization does not adjust for differences in beneficiaries’ health status.
# MAGIC
# MAGIC ## Resources for using and understanding the data
# MAGIC This dataset is based on information gathered from CMS administrative enrollment and claims data for Original Medicare beneficiaries available from the CMS Chronic Conditions Data Warehouse.';

# COMMAND ----------


