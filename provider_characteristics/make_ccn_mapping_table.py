# Databricks notebook source
from pyspark.sql.functions import col, lit, max as _max, min as _min, first, concat_ws

# COMMAND ----------

df = (spark.read.table("mimi_ws_1.datacmsgov.pc_hospital")
    .select("ccn", "npi", "state", "_input_file_date")
    .groupBy("npi", "ccn")
    .agg(_max(col("_input_file_date")).alias("last_observed_date"),
         _min(col("_input_file_date")).alias("first_observed_date"),
         first(col("state")).alias("state"))
    .withColumn("facility_type", lit("hospital")))
    
for facility_type in ["fqhc", "homehealth", "hospice", "ruralhealthclinic", "snf"]:
    df_tmp = (spark.read.table(f"mimi_ws_1.datacmsgov.pc_{facility_type}")
            .select("ccn", "npi", "state", "_input_file_date")
            .groupBy("npi", "ccn")
            .agg(_max(col("_input_file_date")).alias("last_observed_date"),
                _min(col("_input_file_date")).alias("first_observed_date"),
                first(col("state")).alias("state"))
            .withColumn("facility_type", lit(facility_type)))
    df = df.union(df_tmp)

# COMMAND ----------

df = (df.withColumnRenamed("ccn", "other_id")
        .withColumnRenamed("first_observed_date", "other_id_dt_s") 
        .withColumnRenamed("last_observed_date", "other_id_dt_e")
        .withColumn("other_id_type_desc", lit("CCN")) 
        .withColumn("other_id_issuer", lit("MEDICARE"))
        .withColumn("other_id_state", col("state"))
        .withColumn("other_id_type_code", lit("06"))
        .withColumn("other_id_token", concat_ws("^", 
                                          col("other_id"),
                                          col("other_id_type_code"),
                                          col("other_id_state"),
                                          col("other_id_issuer"),
                                          col("other_id_type_desc"),
                                          col("other_id_dt_s"),
                                          col("other_id_dt_e")))
        .select("npi", "other_id_token", "other_id", "other_id_type_code", 
                "other_id_state", "other_id_issuer", "other_id_type_desc", 
                "other_id_dt_s", "other_id_dt_e", "facility_type")
)

# COMMAND ----------

(df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"mimi_ws_1.nppes.otherid_ccn_se"))

# COMMAND ----------


