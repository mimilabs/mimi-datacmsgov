# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Betos mapping file
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
tablename = "betos" # destination table

# COMMAND ----------

files = []
for filepath in Path(f"{path}/{tablename}").glob("*"):
    year = filepath.stem.split("_")[1]
    dt = parse(year + "-12-31").date()
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
    for colname in pdf.columns:
        if colname[-3:] == "_dt":
            pdf[colname] = pd.to_datetime(pdf[colname], 
                                          format="%m/%d/%Y", 
                                          errors="coerce").dt.date
    pdf["_input_file_date"] = item[0]
    pdf_lst.append(pdf)
pdf_full = pd.concat(pdf_lst)

# COMMAND ----------

(spark.createDataFrame(pdf_full).write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.datacmsgov.betos IS '# Restructured BETOS Classification System (RBCS)
# MAGIC
# MAGIC This dataset is from [https://data.cms.gov/provider-summary-by-type-of-service/provider-service-classifications/restructured-betos-classification-system](https://data.cms.gov/provider-summary-by-type-of-service/provider-service-classifications/restructured-betos-classification-system).
# MAGIC
# MAGIC According to the site:
# MAGIC
# MAGIC The Restructured BETOS Classification System (RBCS) dataset is a taxonomy that allows researchers to group healthcare service codes for Medicare Part B services (i.e., HCPCS codes) into clinically meaningful categories and subcategories. It is based on the original Berenson-Eggers Type of Service (BETOS) classification created in the 1980s, and includes notable updates such as Part B non-physician services. The RBCS will undergo annual updates by a technical expert panel of researchers and clinicians.
# MAGIC
# MAGIC This dataset is based on Medicare Part B healthcare service codes. It allows users to group Medicare Part B healthcare service codes into clinically meaningful and consistent categories and subcategories. Users may use the RBCS to analyze trends and perform other types of health services analytic work.';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN hcpcs_cd COMMENT 'HCPCS or CPT code.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_id COMMENT 'RBCS identifier which is comprised of 6 characters. The first character identifies the category; the second character identifies the subcategory; the third, fourth, and fifth characters identify the family, and the sixth character identifies whether the service is a major procedure.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_cat COMMENT 'RBCS Category. First character for the RBCS taxonomy ID, which represents the RBCS ID category.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_cat_desc COMMENT 'RBCS Category Description.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_cat_subcat COMMENT 'RBCS Subcategory First 2 characters for the RBCS taxonomy ID, which represents the RBCS ID\'s category and subcategory.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_subcat_desc COMMENT 'RBCS Subcategory Description';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_famnumb COMMENT 'RBCS Family Number Third, fourth, and fifth characters, which represents the RBCS family number.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_family_desc COMMENT 'RBCS Family Description Family description.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_major_ind COMMENT 'RBCS Major Procedure Indicator. Sixth character for the RBCS taxonomy ID, which identifies whether the HCPCS code is a Major procedure (M), Other (O), or non-procedure code (N).';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN hcpcs_cd_add_dt COMMENT 'HCPCS Code Add Date HCPCS Code Effective Date.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN hcpcs_cd_end_dt COMMENT 'HCPCS Code End Date HCPCS Code End Date.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN rbcs_assignment_eff_dt COMMENT 'RBCS Assignment Effective Date Provides users with the earliest date that the RBCS ID is effective.'; 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.betos ALTER COLUMN _input_file_date COMMENT 'The input file date represents the time range of the dataset. For example, _input_file_date=\'2022-12-31\' represents the data from Year 2022.';

# COMMAND ----------


