from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

@dp.table(
    name="finguard.silver.customers",
    comment="Parsed and cleaned customers data"
)
def customers_silver():
    bronze_df = spark.readStream.table("finguard.bronze.customers")

    transformed_df = bronze_df.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("gender"),
        col("age"),
        col("city"),
        col("state"),
        col("country"),
        col("annual_income"),
        col("customer_segment"),
        col("account_open_date").cast("date"),
        col("risk_score"),
        col("preferred_spending_min"),
        col("preferred_spending_max"),
        col("preferred_city"),
        col("preferred_country"),
        col("trusted_device_id"),
        col("card_number"),
        col("card_type"),
        col("email"),
        col("transaction_limit"),
        col("update_timestamp"),
        current_timestamp().alias("silver_ingestion_timestamp")
    ).filter(col("customer_id").isNotNull())

    return transformed_df
