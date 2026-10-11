# Finguard - Fraud Card Detection

This project is a data engineering pipeline built using Kafka, PySpark, and Databricks to process transaction data and detect potentially fraudulent card transactions.

Transaction data is generated using a Python script and ingested through Kafka. Customer data is loaded from PostgreSQL in batches, while fraud watchlist data is ingested separately. The data is processed through Bronze, Silver, and Gold layers, and the final results are used to create fraud alerts and dashboards.

## Technologies Used

- Python
- Apache Kafka
- PySpark
- Databricks
- Delta Lake
- Lakeflow Spark Declarative Pipelines (DLT)
- PostgreSQL
- Lakeflow Jobs
- Databricks AI/BI Dashboards

## Project Architecture

The project has three main data flows:

1. **Transaction data:** A Python script continuously generates sample transaction data, which is sent to Kafka and ingested into Databricks using Auto Loader.
2. **Customer data:** Customer records are stored in PostgreSQL and loaded into Databricks through a batch pipeline that runs every hour.
3. **Fraud watchlist:** Watchlist data is ingested as a separate streaming pipeline.

The data is processed through Bronze, Silver, and Gold layers, with data quality checks and transformations at different stages.

## Data Processing

### 1. Transaction Data

- Generate sample transaction data using Python.
- Send the data to a Kafka topic.
- Ingest the data into Databricks using Auto Loader.
- Use checkpoints to track streaming progress and a schema location to manage schema inference and evolution.
- Apply data quality checks and transformations.
- Store the processed data in Bronze and Silver tables.

The pipeline is designed to handle around 10 lakh transactions per day.

### 2. Customer Data

- Store customer data in PostgreSQL.
- Run a batch ingestion pipeline every hour.
- Load around 10,000 records per batch.
- Apply transformations and data quality checks before making the data available in the Silver layer.

### 3. Fraud Watchlist

- Ingest fraud watchlist data as a separate stream.
- Process and store the data in streaming tables.
- Use the watchlist data during transaction processing to identify potential fraud cases.

## Fraud Detection

The Gold layer contains the tables used for fraud detection and analysis.

The pipeline uses stream-stream and stream-static joins to combine transaction, customer, and fraud watchlist data.

The main output tables include:

- `fraud_card_alert` – identifies transactions matching fraud watchlist conditions.
- `high_value_transactions_alert` – identifies high-value transactions.
- `transaction_count_by_minute` – tracks transaction counts over time.

These tables can be used to analyze suspicious transactions and monitor transaction activity.

## Data Quality

Data quality checks are applied during pipeline processing to help maintain reliable data.

The pipeline uses expectations and validation rules to check incoming records before processing them further.

## Orchestration and Dashboard

- Use Lakeflow Jobs to orchestrate the ingestion and processing workflows.
- Use Lakeflow Spark Declarative Pipelines to manage streaming transformations and table dependencies.
- Create a Databricks AI/BI dashboard to monitor transaction activity and fraud alerts.

## Project Structure

```text
## Project Structure
Finguard/
├── notebooks/
│   ├── transaction_pipeline
│   └── customer_pipeline
├── pipelines/
│   └── fraud_detection
├── dashboard/
└── README.md
```


*The folder structure above is a suggested layout. Update it to match the actual files in your repository.*

## Key Features

- Kafka-based transaction ingestion
- Batch ingestion from PostgreSQL
- Auto Loader and checkpoint-based streaming
- Bronze, Silver, and Gold data processing
- Data quality checks and schema management
- Stream-stream and stream-static joins
- Fraud alerts and transaction monitoring
- Workflow orchestration using Lakeflow Jobs
- Databricks dashboard for analysis

## How to Run

1. Run the Python script to generate sample transaction data.
2. Start Kafka and send the generated data to the configured topic.
3. Configure the Kafka source and Databricks storage locations.
4. Configure PostgreSQL connectivity for customer data ingestion.
5. Run the required Databricks pipelines.
6. Configure and run the workflows using Lakeflow Jobs.
7. Open the dashboard to view transaction activity and fraud alerts.

Update the connection details, topic names, storage paths, and pipeline settings according to your environment.

## Note

This is a personal data engineering project built to practice streaming ingestion, batch processing, data quality, pipeline orchestration, and fraud detection using Databricks.
