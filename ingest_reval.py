# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Revalidation files
# MAGIC
# MAGIC The data.cms.gov site provides two different types of data: Revaliation List and Revalidation Clinic Group Practice List. 
# MAGIC Based on my examination, the first list is a superset of the latter list. 
# MAGIC Thus, we skip ingesting the latter list.
# MAGIC
# MAGIC From the data, the combined key of `Group Enrollment ID` and `Individual Enrollment ID` makes the unique key of each file. In other words, individual NPIs and Groups may appear multiple times.

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime
from dateutil.parser import parse
import pandas as pd

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "reval" # destination table

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
for filepath in Path(f"{path}/{tablename}").glob("revalidationreassignmentlist*.csv"):
    dt = parse(filepath.stem[-8:]).date()
    if dt not in files_exist:
        files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

pdf_lst = []

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
        if col_new[-5:] == "_date":
            df = df.withColumn(col_new, to_date(col(col_old), format="MM/dd/yyyy"))
        elif col_new in {"group_reassignments_and_physician_assistants", 
                        "individual_total_employer_associations"}:
            df = df.withColumn(col_new, col(col_old).cast(IntegerType()))
        else:
            df = df.withColumn(col_new, col(col_old))
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    (df.write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------


