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

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE mimi_ws_1.datacmsgov.covid19nursinghomes IS '# COVID-19 Nursing Home Data
# MAGIC
# MAGIC Information on COVID-19 reported by nursing homes to the CDC’s National Healthcare Safety Network (NHSN) COVID-19 Long Term Care Facility Module.
# MAGIC
# MAGIC This dataset is from [https://data.cms.gov/covid-19/covid-19-nursing-home-data](https://data.cms.gov/covid-19/covid-19-nursing-home-data).
# MAGIC
# MAGIC According to the site:
# MAGIC
# MAGIC The Nursing Home COVID-19 Public File includes data reported by nursing homes to the CDC’s National Healthcare Safety Network (NHSN) Long Term Care Facility (LTCF) COVID-19 Module: Surveillance Reporting Pathways and COVID-19 Vaccinations. For resources and ways to explore and visualize the data, please see the links to the left, as well as the buttons at the top of the page.
# MAGIC
# MAGIC Up to Date with COVID-19 Vaccines
# MAGIC
# MAGIC On January 1, 2024, the Centers for Disease Control (CDC) changed the way it collects data to calculate the percent of staff who are up to date with their COVID-19 vaccination. It may take facilities some time to adapt to the new methodology. As a result, the reported percent of staff who are up to date with their COVID-19 vaccination should be viewed with caution over the next few weeks. Contact facilities directly for more information on their vaccination levels.';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN week_ending COMMENT 'Last day (MM/DD/YY) of reporting week (a reporting week is from Monday through Sunday).';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN federal_provider_number COMMENT 'The CMS Certification Number (CCN) for the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN provider_name COMMENT 'The provider\'s name.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN provider_address COMMENT 'The provider\'s address.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN provider_city COMMENT 'The provider\'s city.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN provider_state COMMENT 'The provider\'s state.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN provider_zip_code COMMENT 'The provider\'s zip code.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN provider_phone_number COMMENT 'The provider\'s phone number.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN county COMMENT 'The provider\'s county.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN submitted_data COMMENT 'Indicates (Y/N) if any data was submitted for the reporting week.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN passed_quality_assurance_check COMMENT 'Indicates (Y/N) if the data passed the quality assurance check.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN residents_weekly_confirmed_covid19 COMMENT 'Number of residents with new laboratory positive COVID-19 (CONFIRMED) as reported by the provider for this collection date. Note: Numbers for Week Ending 05/24/2020 may include reporting for any time between 01/01/2020 through 05/24/2020. Reporting for subsequent weeks is on a weekly basis.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN residents_total_confirmed_covid19 COMMENT 'Number of residents with laboratory positive COVID-19 (CONFIRMED) since 01/01/2020 as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN residents_weekly_all_deaths COMMENT 'Number of residents who have died in the facility or another location (TOTAL DEATHS) as reported by the provider for this collection date. Note: Numbers for Week Ending 05/24/2020 may include reporting for any time between 01/01/2020 through 05/24/2020. Reporting for subsequent weeks is on a weekly basis.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN residents_total_all_deaths COMMENT 'Number of residents who have died in the facility or another location (TOTAL DEATHS) since 01/01/2020 as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN residents_weekly_covid19_deaths COMMENT 'Number of residents with new suspected or laboratory positive COVID-19 who died in the facility or another location (COVID-19 DEATHS) as reported by the provider for this collection date. Note: Numbers for Week Ending 05/24/2020 may include reporting for any time between 01/01/2020 through 05/24/2020. Reporting for subsequent weeks is on a weekly basis.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN residents_total_covid19_deaths COMMENT 'Number of residents with suspected or laboratory positive COVID-19 who died in the facility or another location (COVID-19 DEATHS) since 1/1/20 as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN number_of_all_beds COMMENT 'Total number of resident beds in the facility as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN total_number_of_occupied_beds COMMENT 'Total number of resident beds that are currently occupied as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN residents_hospitalizations_with_confirmed_covid19 COMMENT 'Number of residents who have been hospitalized with a positive COVID-19 test (residents who have been hospitalized in this reporting period and had a positive COVID-19 test in the 10 days prior to the hospitalization).';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN residents_hospitalizations_with_confirmed_covid19_and_up_to_date_with_vaccines COMMENT 'Number of residents who have been hospitalized with a positive COVID-19 test and also up to date with COVID-19 vaccines at the time of the positive COVID-19 test.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN staff_weekly_confirmed_covid19 COMMENT 'Number of staff and facility personnel with new laboratory positive COVID-19 (CONFIRMED) as reported by the provider for this collection date. Note: Numbers for Week Ending 05/24/2020 may include reporting for any time between 01/01/2020 through 05/24/2020. Reporting for subsequent weeks is on a weekly basis.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN staff_total_confirmed_covid19 COMMENT 'Number of staff and facility personnel with laboratory positive COVID-19 (CONFIRMED) since 01/01/2020 as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN weekly_resident_confirmed_covid19_cases_per_1000_residents COMMENT 'Number of residents with laboratory positive COVID-19 (CONFIRMED) for this collection date per 1,000 residents (Total Number of Occupied Beds) as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN weekly_resident_covid19_deaths_per_1000_residents COMMENT 'Number of residents with suspected or laboratory positive COVID-19 who died in the facility or another location (COVID-19 DEATHS) for this collection date per 1,000 residents as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN total_resident_confirmed_covid19_cases_per_1000_residents COMMENT 'Number of residents with laboratory positive COVID-19 (CONFIRMED) since 01/01/2020 per 1,000 residents (Total Number of Occupied Beds) as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN total_resident_covid19_deaths_per_1000_residents COMMENT 'Number of residents with suspected or laboratory positive COVID-19 who died in the facility or another location (COVID-19 DEATHS) since 1/1/20 per 1,000 residents as reported by the provider.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN number_of_residents_who_are_up_to_date_on_covid19_vaccinations_14_days_or_more_before_positive_test COMMENT 'Same as column heading.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN number_of_residents_staying_in_this_facility_for_at_least_1_day_this_week COMMENT 'Same as column heading.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN number_of_all_healthcare_personnel_eligible_to_work_in_this_facility_for_at_least_1_day_this_week COMMENT 'Same as column heading.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN number_of_residents_staying_in_this_facility_for_at_least_1_day_this_week_up_to_date_with_covid19_vaccines COMMENT 'For current and past definitions, please see: https://www.cdc.gov/nhsn/pdfs/hps/covidvax/UpToDateGuidance-508.pdf';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN recent_percentage_of_current_residents_up_to_date_with_covid19_vaccines COMMENT 'The value of "Percentage of Residents who are Up to Date with COVID-19 Vaccines" for the current week, or if blank, for the prior week.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN percentage_of_current_residents_up_to_date_with_covid19_vaccines COMMENT 'Calculated as follows: (Number of Residents Staying in this Facility for At Least 1 Day This Week Up to Date with COVID-19 Vaccines / Number of Residents Staying in this Facility for At Least 1 Day This Week) * 100';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN number_of_healthcare_personnel_eligible_to_work_in_this_facility_for_at_least_1_day_this_week_up_to_date_with_covid19_vaccines COMMENT 'For current and past definitions, please see: https://www.cdc.gov/nhsn/pdfs/hps/covidvax/UpToDateGuidance-508.pdf';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN recent_percentage_of_current_healthcare_personnel_up_to_date_with_covid19_vaccines COMMENT 'The value of "Percentage of Current Healthcare Personnel who are Up to Date with COVID-19 Vaccines" for the current week, or if blank, for the prior week.';
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN percentage_of_current_healthcare_personnel_up_to_date_with_covid19_vaccines COMMENT 'Calculated as follows: (Number of Healthcare Personnel Eligible to Work in this Facility for At Least 1 Day This Week Up to Date with COVID-19 Vaccines / Number of All Healthcare Personnel Eligible to Work in this Facility for At Least 1 Day This Week) x 100';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE mimi_ws_1.datacmsgov.covid19nursinghomes ALTER COLUMN _input_file_date COMMENT 'The input file date represents the time range of the dataset. The file is weekly updated.';

# COMMAND ----------


