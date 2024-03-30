# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Opt-Out Affidavits
# MAGIC
# MAGIC The input files have a lot of errors (typos, etc.) and duplicates. It needs some amount of cleaning work. 
# MAGIC We first load the data as-is, and then clean up the file a bit. 
# MAGIC Also, the files are relatively small - loadable in memory, so we leverage that side to make things easy.

# COMMAND ----------

!pip install jellyfish

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
from jellyfish import damerau_levenshtein_distance as dld
from jellyfish import soundex as sdx

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

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

pdf_lst = []
for item in files:
    pdf = pd.read_csv(item[1])
    pdf.columns = change_header(pdf.columns)
    pdf["optout_effective_date"] = pd.to_datetime(pdf["optout_effective_date"]).dt.date
    pdf["optout_end_date"] = pd.to_datetime(pdf["optout_end_date"]).dt.date
    pdf["last_updated"] = pd.to_datetime(pdf["last_updated"]).dt.date
    pdf = pdf.sort_values(by=["last_updated", "optout_end_date"], 
                            ascending=False)
    pdf = pdf.drop_duplicates(subset=["npi"], keep="first")
    pdf["_input_file_date"] = item[0]
    pdf_lst.append(pdf)
pdf_full = pd.concat(pdf_lst)

# COMMAND ----------

word_cnt = {}
for index, row in pdf_full.groupby('specialty').count().loc[:,["npi"]].iterrows():
    word_cnt[index] = row['npi']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spell Correction for Medicine!

# COMMAND ----------

mapping = {}
mapping_reverse = {}
for word_a, score_a in word_cnt.items():
    for word_b, score_b in word_cnt.items():
        if word_a == word_b:
            continue
        distance = dld(word_a, word_b)
        if (distance < 3 and 
            score_a > score_b and 
            sdx(word_a) == sdx(word_b)):
            if ((word_b not in mapping) or
                (score_a > mapping[word_b]["score_a"])):
                doc = {"a": word_a,
                        "b": word_b,
                        "score_a": score_a,
                        "score_b": score_b}
                mapping[word_b] = doc
                mapping_reverse[word_a] = doc

for key_reverse, doc_reverse in mapping_reverse.items():
    if key_reverse in mapping:
        word_a = doc_reverse["a"]
        word_b = doc_reverse["b"]
        mapping[word_b]["a"] = mapping[word_a]["a"]
        mapping[word_b]["score_a"] = mapping[word_a]["score_a"]

replace_map = {k:k for k in word_cnt.keys()}
replace_map = {**replace_map, **{k:v["a"] for k, v in mapping.items()}}

# COMMAND ----------

pdf_full["specialty"] = pdf_full["specialty"].map(replace_map)

# COMMAND ----------

(spark.createDataFrame(pdf_full).write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------


