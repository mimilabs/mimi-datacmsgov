# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Opioid Treatment Program Providers
# MAGIC

# COMMAND ----------

from pathlib import Path
import csv
from pyspark.sql.functions import col, lit, to_date, split, explode, collect_set
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import re

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "otpp" # destination table

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
    datestr = filepath.stem.split("_")[-1]
    if datestr == "20231204":
        dt = datetime.strptime(datestr, "%Y%m%d").date()
    else:
        dt = datetime.strptime(datestr, "%m%d%Y").date()
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
        if col_new == "medicare_id_effective_date":
            df = df.withColumn(col_new, to_date(col(col_old), format="M/d/yyyy"))
        elif col_new == "npi":
            df = df.withColumn(col_new, col(col_old))
            df = df.withColumn("npi_lst", split(col(col_old), pattern=" "))
            header.append("npi_lst")
        else:
            df = df.withColumn(col_new, col(col_old))
        
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0]))
          .select(explode(col("npi_lst")).alias("npi"),
                  "provider_name",
                    "address_line_1",
                    "address_line_2",
                    "city",
                    "state",
                    "zip",
                    "medicare_id_effective_date",
                    "phone",
                    "_input_file_date")
            .dropDuplicates())
    
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.datacmsgov.otpp IS '# Opioid Treatment Program Providers
# MAGIC
# MAGIC A list of Providers enrolled in the Opioid Treatment Program under Medicare.
# MAGIC
# MAGIC This dataset is from [https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/opioid-treatment-program-providers](https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/opioid-treatment-program-providers).
# MAGIC
# MAGIC According to the site:
# MAGIC
# MAGIC >The Opioid Treatment Program (OTP) Providers dataset provides information on Providers who have enrolled in Medicare under the Opioid Treatment Program. It contains provider\'s name, National Provider Identifier (NPI), address, phone number and the effective enrollment date.';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN npi COMMENT 'National Provider Identifier (NPI) number of the Provider';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN provider_name COMMENT 'Name of the Provider';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN address_line_1 COMMENT 'Provider\'s Street Address';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN address_line_2 COMMENT 'Provider\'s Street Address';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN city COMMENT 'Provider\'s City';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN state COMMENT 'Provider\'s State Abbreviation';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN zip COMMENT 'Provider\'s Zip Code';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN medicare_id_effective_date COMMENT 'The date when the Provider\'s Medicare ID becomes effective';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN phone COMMENT 'Provider\'s Phone number';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.otpp ALTER COLUMN _input_file_date COMMENT 'The input file date represents the time range of the dataset. The file is weekly updated.';

# COMMAND ----------


