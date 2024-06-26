# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly Download
# MAGIC
# MAGIC We download: weekly, monthly, annually updated datasets every week.

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

messy_fn_to_clean_fn = {"Medicare%20Durable%20Medical%20Equipment%2C%20Devices%20%26%20Supplies%20-%20by%20Referring%20Provider%20and%20Service%20Data%202013.csv": "mup_dme_r19_p08_v10_d13_prvhpr.csv",
"Medicare%20Durable%20Medical%20Equipment%2C%20Devices%20%26%20Supplies%20-%20by%20Referring%20Provider%20and%20Service%20Data%202014_0.csv": "mup_dme_r19_p08_v10_d14_prvhpr.csv",
"Medicare%20Durable%20Medical%20Equipment%2C%20Devices%20%26%20Supplies%20-%20by%20Referring%20Provider%20and%20Service%20Data%202015_0.csv": "mup_dme_r19_p08_v10_d15_prvhpr.csv",
"Medicare%20Durable%20Medical%20Equipment%2C%20Devices%20%26%20Supplies%20-%20by%20Referring%20Provider%20and%20Service%20Data%202016.csv": "mup_dme_r19_p08_v10_d16_prvhpr.csv",
"Medicare%20Durable%20Medical%20Equipment%2C%20Devices%20%26%20Supplies%20-%20by%20Referring%20Provider%20and%20Service%20Data%202017.csv": "mup_dme_r19_p08_v10_d17_prvhpr.csv",
"Medicare%20Durable%20Medical%20Equipment%2C%20Devices%20%26%20Supplies%20-%20by%20Referring%20Provider%20Data%202015.csv": "mup_dme_r19_p08_v10_d15_prvr.csv",
"Medicare%20Durable%20Medical%20Equipment%2C%20Devices%20%26%20Supplies%20-%20by%20Referring%20Provider%20Data%202016.csv": "mup_dme_r19_p08_v10_d16_prvr.csv",
"Medicare%20Durable%20Medical%20Equipment%2C%20Devices%20%26%20Supplies%20-%20by%20Referring%20Provider%20Data%202017.csv": "mup_dme_r19_p08_v10_d17_prvr.csv"}

filenames = [messy_fn_to_clean_fn.get(url.split("/")[-1], url.split("/")[-1])
             for url in download_urls]
download_files(download_urls, volumepath, "mupdme", filenames=filenames)

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

# MAGIC %md
# MAGIC ## Outpatient

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Outpatient Hospitals - by"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
download_files(download_urls, volumepath, "mupohp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Post-acute/Hospice

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Post-Acute Care and Hospice - by"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
download_files(download_urls, volumepath, "muppac")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inpatient

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Inpatient Hospitals - by"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
download_files(download_urls, volumepath, "mupihp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## National Geographic Variation

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("Medicare Geographic Variation"))
                    .toPandas())
download_urls = pdf["downloadURL"].to_list()
messy_fn_to_clean_fn = {"2014-2022%20Medicare%20FFS%20Geographic%20Variation%20Public%20Use%20File.csv": "medicare_geographic_variation_2022.csv",
"Geographic%20Variation%20Public%20Use%20File%20State%20County.csv": "medicare_geographic_variation_2021.csv"}
filenames = [messy_fn_to_clean_fn.get(url.split("/")[-1], url.split("/")[-1])
             for url in download_urls]
download_files(download_urls, volumepath, "geovariation", filenames=filenames)

# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID-19 Nursing Homes

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.datacmsgov.datacatalog")
                    .filter(col("mediaType")=="text/csv")
                    .filter(col("title")
                        .contains("COVID-19 Nursing Home Data"))
                    .toPandas())
# CMS "overwrites" the data file every week. However, we keep the weekly file separately.
download_urls = pdf["downloadURL"].to_list()
today = datetime.today().strftime('%Y-%m-%d')
fn = f"covid19_nursing_home_data_{today}.csv"
download_file(download_urls[0], fn, volumepath, "covid19nursinghomes")
