from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp, concat_ws, lit
import pyspark.sql.functions as F


@dp.table(
    name = "finguard.gold.transaction_count_by_minute_sliding_window",
    comment = "Alert details where transaction has been performed with value higher then what is configured by customer"
)
def transaction_count_by_minute():

    transactions_df = spark.readStream.table("finguard.silver.transactions")
    transactions_df = transactions_df.withColumn("transaction_timestamp", col("transaction_timestamp").cast("timestamp"))

    transactions_with_watermark = transactions_df.withWatermark("transaction_timestamp", "5 minutes")

    transactions_count_df = transactions_with_watermark.groupBy(
            F.window("transaction_timestamp", "5 minute","1 minute"),
            "customer_id"
        ).agg(
            F.count("*").alias("transaction_count")
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("transaction_count")
        )
    return transactions_count_df

    