# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the COVID-19 Nursing Home Data
# MAGIC

# COMMAND ----------

from pathlib import Path
import csv
from pyspark.sql.functions import col, lit, to_date
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import re

path = "/Volumes/mimi_ws_1/datacmsgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datacmsgov" # delta table destination schema
tablename = "covid19nursinghomes" # destination table

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

files = []
for filepath in Path(f"{path}/{tablename}").glob("*.csv"):
    datestr = filepath.stem.split("_")[-1]
    dt = parse(datestr).date()
    files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)
file = files[0] # always overwrite with the latest

# COMMAND ----------

int_patterns = ["residents_weekly_confirmed_covid19",
                   "residents_total_confirmed_covid19", 
                   "residents_weekly_all_deaths",
                    "residents_total_all_deaths",
                    "residents_weekly_covid19_deaths",
                    "residents_total_covid19_deaths",
                    "number_of_all_beds",
                    "total_number_of_occupied_beds",
                    "residents_hospitalizations_with_confirmed_covid19", 
                    "residents_hospitalizations_with_confirmed_covid19_and_up_to_date_with_vaccines",
                    "staff_weekly_confirmed_covid19",
                    "staff_total_confirmed_covid19",
                    "staff_total_confirmed_covid19",
                    "number_of_residents_who_are_up_to_date_on_covid19_vaccinations_14_days_or_more_before_positive_test",
                    "number_of_residents_staying_in_this_facility_for_at_least_1_day_this_week",
                    "number_of_all_healthcare_personnel_eligible_to_work_in_this_facility_for_at_least_1_day_this_week",
                    "number_of_residents_staying_in_this_facility_for_at_least_1_day_this_week_up_to_date_with_covid19_vaccines",
                    "number_of_healthcare_personnel_eligible_to_work_in_this_facility_for_at_least_1_day_this_week_up_to_date_with_covid19_vaccines"
                    ]
double_patterns = ["weekly_resident_confirmed_covid19_cases_per_1000_residents",
                   "weekly_resident_covid19_deaths_per_1000_residents",
                   "total_resident_confirmed_covid19_cases_per_1000_residents",
                   "total_resident_covid19_deaths_per_1000_residents",
                   "recent_percentage_of_current_residents_up_to_date_with_covid19_vaccines",
                   "percentage_of_current_residents_up_to_date_with_covid19_vaccines",
                   "recent_percentage_of_current_healthcare_personnel_up_to_date_with_covid19_vaccines",
                   "percentage_of_current_healthcare_personnel_up_to_date_with_covid19_vaccines"
                   ]

df = (spark.read.format("csv")
        .option("header", "true")
        .load(str(file[1])))
header = []
for col_old, col_new in zip(df.columns, change_header(df.columns)):
    header.append(col_new)

    if col_new in double_patterns:
        df = df.withColumn(col_new, col(col_old).cast("double")) 
    elif col_new in int_patterns:
        df = df.withColumn(col_new, col(col_old).cast("int")) 
    elif col_new == "week_ending":
        df = df.withColumn(col_new, to_date(col(col_old), format="MM/dd/yy"))
    else:
        df = df.withColumn(col_new, col(col_old))
    
df = (df.select(*header)
        .withColumn("_input_file_date", lit(file[0])))

(df.write
    .format('delta')
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------


