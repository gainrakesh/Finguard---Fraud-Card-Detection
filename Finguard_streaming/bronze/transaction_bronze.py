from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp
import json

@dp.table(
    name = "finguard.bronze.transactions",
    comment = " Transactions Raw stream data from Kafka"
)
def transactions_bronze():

    kafka_connection_json = dbutils.secrets.get(scope='finguard-scope',key = 'kafka_connection_details')
    kafka_config = json.loads(kafka_connection_json)
    BOOTSTRAP_SERVERS = kafka_config['bootstrap_servers']
    API_KEY = kafka_config['api_key']
    API_SECRET = kafka_config['api_secret']
    TOPIC_NAME = kafka_config['topic']

    jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{API_KEY}' password='{API_SECRET}';"

    streaming_df = (spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
                    .option("subscribe", TOPIC_NAME)
                    .option("kafka.security.protocol", "SASL_SSL")
                    .option("kafka.sasl.mechanism", "PLAIN")
                    .option("kafka.sasl.jaas.config",jaas_config)
                    .option("startingOffsets","earliest")
                    .load()
    )

    
    parsed_streaming_df = streaming_df.select(col("key").cast("string"), col("value").cast("string"), col("topic"), col("partition"), col("offset"), col("timestamp"), col("timestampType") , current_timestamp().alias("ingest_ts"))

    return parsed_streaming_df
