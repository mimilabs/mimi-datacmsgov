# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Medicare-enrolled Facilities
# MAGIC

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime
from dateutil.parser import parse
import pandas as pd

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# https://resdac.org/cms-data/variables/provider-base-facility-cms-certification-number-ccn
# Apparently, there are some legacy numberings that need to be cleaned up for CCN
def get_ccn_cleaned(ccn):
    if not isinstance(ccn, str):
        return ""
    elif ccn[2] in {"M", "R", "Z"} and ccn[3].isnumeric():
        return ccn[:2] + "1" + ccn[3:6]
    elif ccn[2] in {"S", "T", "U"} and ccn[3].isnumeric():
        return ccn[:2] + "0" + ccn[3:6]
    elif ccn[2] == "W" and ccn[3].isnumeric():
        return ccn[:2] + "2" + ccn[3:6]
    elif ccn[2] == "Y" and ccn[3].isnumeric():
        return ccn[:2] + "3" + ccn[3:6]
    elif ccn[2:4] in {"TA", "SA"}:
        return ccn[:2] + "20" + ccn[4:6]
    elif ccn[2:4] in {"TB", "SB"}:
        return ccn[:2] + "21" + ccn[4:6]
    elif ccn[2:4] in {"TC", "SC"}:
        return ccn[:2] + "22" + ccn[4:6]
    elif ccn[2:4] == "SD":
        return ccn[:2] + "30" + ccn[4:6]
    elif ccn[2:4] in {"TE", "SE"}:
        return ccn[:2] + "33" + ccn[4:6]
    elif ccn[2:4] == "TF":
        return ccn[:2] + "40" + ccn[4:6]
    elif ccn[2:4] == "TG":
        return ccn[:2] + "41" + ccn[4:6]
    elif ccn[2:4] == "TH":
        return ccn[:2] + "42" + ccn[4:6]
    elif ccn[2:4] == "TJ":
        return ccn[:2] + "43" + ccn[4:6]
    elif ccn[2:4] == "TK":
        return ccn[:2] + "44" + ccn[4:6]
    else:
        return ccn[:6]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest!!

# COMMAND ----------

tables = ["pc_hospital", "pc_snf", "pc_homehealth", 
          "pc_fqhc", "pc_hospice", "pc_ruralhealthclinic"]

# COMMAND ----------

for tablename in tables:
    
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
    files = []
    for filepath in Path(f"{path}/{tablename}").glob("*"):
        dt = parse("2022-11-30").date() # the filename is missing the date modifier for this date: 2022-11-30
        if filepath.stem[-10:].replace('.', '').isnumeric():
            dt = parse(filepath.stem[-10:].replace('.', '-')).date()
        if dt not in files_exist:
            files.append((dt, filepath))
    files = sorted(files, key=lambda x: x[0], reverse=True)

    for item in files:
        # each file is small enough (200~300MB) to load in memory
        pdf = pd.read_csv(item[1], encoding='ISO-8859-1', dtype=str)
        pdf.columns = change_header(pdf.columns)
        for colname in pdf.columns:
            if colname[-5:] == "_date":
                pdf[colname] = pd.to_datetime(pdf[colname]).dt.date
        pdf["ccn_remapped"] = pdf["ccn"].apply(get_ccn_cleaned)
        pdf["_input_file_date"] = item[0]
        df = spark.createDataFrame(pdf)
        (df.write
            .format('delta')
            .mode(writemode)
            .saveAsTable(f"{catalog}.{schema}.{tablename}"))
        writemode="append"
    
    print(f"{tablename} done!!")

# COMMAND ----------


