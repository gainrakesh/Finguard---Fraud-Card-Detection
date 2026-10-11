from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
import json

@dp.table(
    name = "finguard.silver.fraud_watchlist",
    comment = "Cleaned fraud watchlist" 
)
def fraud_watchlist_silver():

    source_path = "finguard.bronze.fraud_watchlist"
    bronze_df = spark.readStream.table(source_path)

    
    cleaned_df = bronze_df.select(
        upper(col("watchlist_id")).alias("watchlist_id"),
        col("watch_type"),
        upper(col("entity_id")).alias("entity_id"),
        upper(col("risk_level")).alias("risk_level"),
        upper(col("action")).alias("action"),
        col("reason_code"),
        col("reason_description"),
        col("status"),
        to_timestamp(col("effective_from"),"dd-MM-yyyy HH:mm:ss").alias("effective_from"),
        col("reported_by"),
        col("country"),
        col("city"),
        col("file_path"),
        col("ingested_timestamp").alias("bronze_ingested_timestamp"),
        current_timestamp().alias("silver_ingested_timestamp")
    )

    return cleaned_df
