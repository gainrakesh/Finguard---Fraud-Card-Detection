from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp
import json

@dp.table(
    name = "finguard.bronze.fraud_watchlist",
    comment = "Fraud watchlist raw stream data ingested by auto loader" 
)
def fraud_watchlist_bronze():

    source_path = '/Volumes/finguard/source/fraud_watchlist/source_data/'
    streaming_df = (spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", "json")
                    .option("cloudFiles.inferColumnTypes","true")
                    .load(source_path)
                )

    
    parsed_streaming_df = streaming_df.select(
        col("watchlist_id"),
        col("watch_type"),
        col("entity_id"),
        col("risk_level"),
        col("action"),
        col("reason_code"),
        col("reason_description"),
        col("status"),
        col("effective_from"),
        col("reported_by"),
        col("country"),
        col("city"),
        col("_metadata.file_path"),
        col("_rescued_data"),
        current_timestamp().alias("ingested_timestamp")
    )

    return parsed_streaming_df
