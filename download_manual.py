# Databricks notebook source
# MAGIC %md
# MAGIC ## Manual Download
# MAGIC
# MAGIC Datasets do not get updated often.

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

# MAGIC %md
# MAGIC ## BETOS Mapping

# COMMAND ----------


pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Restructured BETOS Classification System"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
download_file(download_urls[0], "rbcs_2023_taxonomy.csv", volumepath, "betos")
