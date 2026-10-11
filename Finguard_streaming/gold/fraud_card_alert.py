from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, concat_ws, lit


@dp.table(
    name="finguard.gold.fraud_card_alert",
    comment="Alert details where a transaction has been performed with a value higher than what is configured by the customer"
)
def fraud_card_alert():

    # Read source tables
    transactions_df = spark.readStream.table(
        "finguard.silver.transactions"
    )

    fraud_watchlist = spark.read.table(
        "finguard.silver.fraud_watchlist"
    )

    customers_df = spark.read.table(
        "finguard.silver.customers"
    )

    transactions_with_watermark = transactions_df.withColumn("_transaction_ts", col("transaction_timestamp").cast("timestamp")).withWatermark("_transaction_ts", "5 minutes")
    fraud_watchlist_with_watermark = fraud_watchlist.withWatermark("effective_from", "5 minutes")
    # Apply aliases to avoid ambiguous column references
    transactions_with_watermark = transactions_with_watermark.alias("t")
    fraud_watchlist_with_watermark = fraud_watchlist_with_watermark.alias("w")
    customers_df = customers_df.alias("c")

    # Join transactions with the fraud watchlist and customer details
    fraud_detected = (
        transactions_with_watermark
        .join(
            fraud_watchlist_with_watermark,
            col("t.card_number") == col("w.entity_id"),
            "inner"
        )
        .join(
            customers_df,
            col("c.customer_id") == col("t.customer_id"),
            "left"
        )
        .select(
            # Alert identification
            concat_ws(
                "-",
                lit("FRAUD"),
                col("t.transaction_id"),
                col("w.watchlist_id")
            ).alias("alert_id"),

            lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),

            current_timestamp().alias("alert_timestamp"),

            # Transaction details
            col("t.transaction_id"),
            col("t.customer_id"),
            col("c.email").alias("customer_email"),

            concat_ws(
                " ",
                col("c.first_name"),
                col("c.last_name")
            ).alias("customer_name"),

            col("t.card_number"),
            col("t.amount"),
            col("t.currency"),
            col("t.merchant_id"),
            col("t.merchant_name"),
            col("t.merchant_category"),
            col("t.transaction_type"),
            col("t.payment_channel"),
            col("t.device_id"),
            col("t.city").alias("transaction_city"),
            col("t.country").alias("transaction_country"),
            col("t.transaction_timestamp"),
            col("t.is_international"),
            col("t.status").alias("transaction_status"),

            # Fraud watchlist details
            col("w.watchlist_id"),
            col("w.watch_type"),
            col("w.risk_level"),
            col("w.action"),
            col("w.reason_code"),
            col("w.reason_description"),
            col("w.effective_from").alias("watchlist_effective_from"),
            col("w.reported_by"),
            col("w.city").alias("watchlist_city"),
            col("w.country").alias("watchlist_country")
        )
    )

    return fraud_detected