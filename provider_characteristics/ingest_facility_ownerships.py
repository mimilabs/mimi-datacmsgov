# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Facility Ownership data
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

# MAGIC %md
# MAGIC ## Ingest!

# COMMAND ----------

tables = ["pc_hospital_owner", "pc_snf_owner", "pc_homehealth_owner", 
          "pc_fqhc_owner", "pc_hospice_owner", "pc_ruralhealthclinic_owner"]

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
            if "_date_" in colname:
                pdf[colname] = pd.to_datetime(pdf[colname]).dt.date
        pdf["_input_file_date"] = item[0]
        df = spark.createDataFrame(pdf)
        (df.write
            .format('delta')
            .mode(writemode)
            .saveAsTable(f"{catalog}.{schema}.{tablename}"))
        writemode="append"
    
    print(f"{tablename} done!!")

# COMMAND ----------


