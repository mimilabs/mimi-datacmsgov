# Databricks notebook source
from pathlib import Path
import re
import csv
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime
from dateutil.parser import parse

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "optout" # destination table

# COMMAND ----------

files = []
for filepath in Path(f"{path}/{tablename}").glob("*"):
    res = re.search(r"_(\w+)(\d{4})", filepath.stem)
    dt = parse(res.group(1) + " 01, " + res.group(2)).date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

files_exist = {}
# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
files_to_ingest = [item for item in files if item[0] not in files_exist]

# COMMAND ----------

header = []
if len(files) > 0:
    for column in next(csv.reader(open(str(files[0][1]), "r"))):
        col_new = re.sub(r'\W+', '', column.lower().replace(' ','_'))
        if col_new[-5:] == "_date":
            header.append(StructField(col_new + "_", StringType(), True))
        elif col_new == "last_updated":
            header.append(StructField(col_new+"_date_", StringType(), True))
        else:
            header.append(StructField(col_new, StringType(), True))
    #header.append(StructField("_input_file_date", DateType(), True))
table_schema = StructType(header)

# COMMAND ----------

header

# COMMAND ----------

writemode = "overwrite"
for item in sorted(files_to_ingest, key=lambda x: x, reverse=True):
    df = (spark.read.format("csv")
            .option("header", "false")
            .option("skipRows", "1")
            .schema(table_schema)
            .load(str(item[1]))
            .withColumn('_input_file_date', lit(item[0]))
            .withColumn('optout_effective_date', 
                        to_date(col('optout_effective_date_'), 
                                'MM/dd/yyyy'))
            .withColumn('optout_end_date', 
                        to_date(col('optout_end_date_'), 
                                'MM/dd/yyyy'))
            .withColumn('last_updated_date', 
                        to_date(col('last_updated_date_'), 
                                'MM/dd/yyyy'))
            .drop('optout_effective_date_', 'optout_end_date_', 'last_updated_date_'))
    
    if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
        writemode = "append"
    
    (df.write
        .format('delta')
        .mode(writemode)
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------


