# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Download Process
# MAGIC
# MAGIC We update the data catalog every day.
# MAGIC
# MAGIC Also, we update semi-weekly datasets every day.

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

import requests
import json
import pandas as pd
from pyspark.sql.functions import col
from tqdm import tqdm
from pathlib import Path
import re
from datetime import datetime

url = "https://data.cms.gov/data.json"
catalog = "mimi_ws_1"
schema = "datacmsgov"
tablename = "datacatalog"
volumepath = "/Volumes/mimi_ws_1/datacmsgov/src"

# COMMAND ----------

def download_file(url, filename, path, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{path}/{folder}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

def download_files(urls, path, folder, filenames = None):

    for i, download_url in enumerate(urls):
        filename = download_url.split("/")[-1]
        if filenames is not None:
            filename = filenames[i]
        # Check if the file exists
        if Path(f"{path}/{folder}/{filename}").exists():
            #print(f"{filename} already exists, skipping...")
            continue
        else:
            print(f"{filename} downloading...")
            download_file(download_url, filename, path, folder)

# COMMAND ----------

res = requests.get(url)

# COMMAND ----------

header1 = ["accessLevel",
           "bureauCode", 
          "accrualPeriodicity", 
          "describedBy", 
          "description"]
header2 = ["format",
          "downloadURL",
          "accessURL",
          "mediaType",
          "title",
          "modified",
          "temporal"]
data = []
for d in res.json()["dataset"]:
    row1 = []
    for k1 in header1:
        row1.append(d.get(k1, ""))
    for dd in d["distribution"]:
        row2 = []
        for k2 in header2:
            row2.append(dd.get(k2, ""))
        data.append(row1 + row2)

# COMMAND ----------


(spark.createDataFrame(pd.DataFrame(data, 
                                   columns=header1 + header2))
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Order and Referring

# COMMAND ----------


pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Order and Referring"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
download_files(download_urls, volumepath, "orderandreferring")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pending Initial Logging and Tracking

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Pending Initial Logging"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
download_files(download_urls, volumepath, "pendingilt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opioid Treatment Program Providers

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Opioid Treatment Program Providers"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
download_files(download_urls, volumepath, "otpp")

# COMMAND ----------


