# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Initial Logging and Tracking files
# MAGIC

# COMMAND ----------

from pathlib import Path
import csv
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import re

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "pendingilt" # destination table

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

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
    datestr = filepath.stem.split("_")[1]
    dt = parse(datestr).date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    # each file is relatively small enough (~600MB)
    # but the size is increasing every month
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):
        header.append(col_new)
        df = df.withColumn(col_new, col(col_old))
        
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
# MAGIC COMMENT ON TABLE mimi_ws_1.datacmsgov.pendingilt IS '# Pending Initial Logging and Tracking
# MAGIC
# MAGIC A list of applications pending CMS contractor review for both physicians and non-physicians.
# MAGIC
# MAGIC This dataset is from [https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/pending-initial-logging-and-tracking-non-physicians](https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/pending-initial-logging-and-tracking-non-physicians) and [https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/pending-initial-logging-and-tracking-physicians](https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/pending-initial-logging-and-tracking-physicians).
# MAGIC
# MAGIC According to the site:
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC The logic is to get all the PECOS L&Ts with the submittal reason of “Initial Enrollment” for 855I and 855O and in an open status (not finalized). The PAC_ID of that initial L&T should not have an Approved enrollment or Opt-Out affidavit.  It is for physicians and non-physicians enrolling for the first time and pending. We generate separate files for physicians and non-physicians.
# MAGIC
# MAGIC ## Pending Initial L&Ts Physicians
# MAGIC
# MAGIC The Initial Physician Applications Pending Contractor Review files are lists of applications pending contractor review. These pending applications have NOT been processed by the CMS contractors. These lists have been compiled to allow individuals the ability to verify that an application has been submitted and is awaiting processing.
# MAGIC
# MAGIC ## Pending Initial L&Ts Non-Physicians
# MAGIC
# MAGIC The new Initial Non-Physician Applications Pending Contractor Review files are lists of applications pending contractor review. These pending applications have NOT been processed by the CMS contractors. These lists have been compiled to allow individuals the ability to verify that an application has been submitted and is awaiting processing.';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.pendingilt ALTER COLUMN npi COMMENT 'National Provider Identifier (NPI) number';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.pendingilt ALTER COLUMN last_name COMMENT 'Last Name of the Provider';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.pendingilt ALTER COLUMN first_name COMMENT 'First Name of the Provider';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.pendingilt ALTER COLUMN _input_file_date COMMENT 'The input file date represents the time range of the dataset. The file is weekly updated.';

# COMMAND ----------


