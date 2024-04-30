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

catalog = "mimi_ws_1"
schema = "datacmsgov"
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
# MAGIC
# MAGIC ## Facilities

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Hospital Enrollments"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_hospital")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Hospice Enrollments"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_hospice")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Home Health Agency Enrollments"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_homehealth")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Federally Qualified Health Center Enrollments"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_fqhc")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Rural Health Clinic Enrollments"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_ruralhealthclinic")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Skilled Nursing Facility Enrollments"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_snf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Providers

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Fee-For-Service Public Provider Enrollment"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_provider")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ownerships

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Hospital All Owners"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_hospital_owner")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Hospice All Owners"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_homehealth")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Home Health Agency All Owners"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_homehealth_owner")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Federally Qualified Health Center All Owners"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_fqhc_owner")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Rural Health Clinic All Owners"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_ruralhealthclinic_owner")

# COMMAND ----------

download_urls = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Skilled Nursing Facility All Owners"))
                    .toPandas()["downloadURL"].to_list())
download_files(download_urls, volumepath, "pc_snf_owner")

# COMMAND ----------


