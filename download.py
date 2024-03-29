# Databricks notebook source
!pip install tqdm

# COMMAND ----------

import requests
import json
import pandas as pd
from pyspark.sql.functions import col
from tqdm import tqdm
from pathlib import Path
import re

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
            print(f"{filename} already exists, skipping...")
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
# MAGIC ## PPEF

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Fee-For-Service Public Provider Enrollment"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "ppef")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## MUP - PHY

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Physician & Other Practitioners - by"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "mupphy")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MUP - DPR
# MAGIC

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Part D Prescribers - by"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "mupdpr")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## MUP - DME

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Durable Medical Equipment, Devices & Supplies - by"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "mupdme")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reval

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Revalidation Reassignment List"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
filenames = [re.sub("\W+", "", x.lower()) + ".csv" for x in pdf["title"].to_list()]
download_files(download_urls, volumepath, "reval", filenames)

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Revalidation Clinic Group Practice Reassignment"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
filenames = [re.sub("\W+", "", x.lower()) + ".csv" for x in pdf["title"].to_list()]
download_files(download_urls, volumepath, "reval", filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Optout

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Opt Out Affidavits"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
download_files(download_urls, volumepath, "optout")

# COMMAND ----------


