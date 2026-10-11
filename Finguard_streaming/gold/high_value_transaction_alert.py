from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp, concat_ws, lit


@dp.table(
    name = "finguard.gold.high_value_transactions_alert",
    comment = "Alert details where transaction has been performed with value higher then what is configured by customer"
)
def high_value_transactions_alert():

    transactions_df = spark.readStream.table("finguard.silver.transactions")
    customers_df = spark.read.table("finguard.silver.customers")

    joined_df = (transactions_df.join(customers_df, transactions_df.customer_id == customers_df.customer_id, "left")
                                .filter(col("amount") > col("transaction_limit"))
                                .select(
                                    concat_ws("-",lit("ALERT"),col("transaction_id")).alias("alert_id"),
                                    lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
                                    current_timestamp().alias("alert_timestamp"),
                                    transactions_df.transaction_id.alias("transaction_id"),
                                    transactions_df.customer_id.alias("customer_id"),
                                    customers_df.email.alias("email"),
                                    concat_ws(" ",col("first_name"),col("last_name")).alias("customer_name"),
                                    transactions_df.amount.alias("amount"),
                                    transactions_df.transaction_type.alias("transaction_type"),
                                    transactions_df.status.alias("transaction_status"),
                                    transactions_df.payment_channel.alias("transaction_channel"),
                                    transactions_df.country.alias("transaction_country"),
                                    transactions_df.city.alias("transaction_city")
                                )
    )
    return joined_df