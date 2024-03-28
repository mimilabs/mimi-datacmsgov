# Databricks notebook source
import requests
import json
import pandas as pd

url = "https://data.cms.gov/data.json"
catalog = "mimi_ws_1"
schema = "datacmsgov"
tablename = "datacatalog"

# COMMAND ----------

res = requests.get(url)

# COMMAND ----------

header1 = ["accessLevel",
           "bureauCode" 
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


