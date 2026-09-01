"""

Full FinShield Spark Structured Streaming pipeline.

Stage A - Bronze ingestion (both sources, raw passthrough)
Stage B - Reference data load (latest merchant risk record per merchant)
Stage C - Enrichment join (stream-static join on 'merchant')
Stage D - PII masking (DataShield module)
Stage E - Fraud detection logic (velocity, amount, merchant risk)
Stage F - Write enriched/flagged/masked data to Silver

Design note: velocity/spend-spike signals are computed per micro-batch
(via foreachBatch) rather than as a continuous windowed stream-stream join.
This is a deliberate simplification - stream-stream joins with watermarking
add significant complexity for a modest accuracy gain at this scale, and a
stream-static join (per micro-batch, against periodically refreshed
reference data) is the standard, defensible pattern for this kind of
enrichment. See README "Design Decisions".
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    from_json, col, sha2, regexp_replace, row_number, count, avg,
    when, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)

load_dotenv()

# =========================================================================
# Schemas
# =========================================================================

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("cc_num", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amt", DoubleType(), True),
    StructField("customer_first_name", StringType(), True),
    StructField("customer_last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("customer_lat", DoubleType(), True),
    StructField("customer_long", DoubleType(), True),
    StructField("city_pop", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("merch_lat", DoubleType(), True),
    StructField("merch_long", DoubleType(), True),
    StructField("is_fraud", IntegerType(), True),
])

merchant_schema = StructType([
    StructField("merchant", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("is_blacklisted", BooleanType(), True),
    StructField("last_flagged_date", StringType(), True),
    StructField("compliance_status", StringType(), True),
    StructField("last_updated", StringType(), True),
])

# =========================================================================
# Snowflake connection options
# =========================================================================

def sf_options(schema: str) -> dict:
    return {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": schema,
    }

BRONZE_TXN_CHECKPOINT = "spark_jobs/checkpoints/bronze_transactions"
BRONZE_MERCH_CHECKPOINT = "spark_jobs/checkpoints/bronze_merchant_updates"
SILVER_CHECKPOINT = "spark_jobs/checkpoints/silver_transactions"

# =========================================================================
# Spark session
# =========================================================================

spark = SparkSession.builder \
    .appName("FinShield-StreamingPipeline") \
    .master("local[1]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.pyspark.driver.python", "python") \
    .config("spark.pyspark.python", "python") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def write_snowflake(df, schema: str, table: str, mode: str = "append"):
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options(schema)) \
        .option("dbtable", table) \
        .mode(mode) \
        .save()


def read_snowflake(schema: str, table: str):
    return spark.read \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options(schema)) \
        .option("dbtable", table) \
        .load()


# =========================================================================
# Stage A - Bronze ingestion
# =========================================================================

txn_raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw.transactions") \
    .option("startingOffsets", "earliest") \
    .load()

txn_parsed_stream = txn_raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), transaction_schema).alias("data")) \
    .select("data.*")

merch_raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw.merchant_updates") \
    .option("startingOffsets", "earliest") \
    .load()

merch_parsed_stream = merch_raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), merchant_schema).alias("data")) \
    .select("data.*")


def write_bronze_transactions(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    write_snowflake(batch_df, "BRONZE", "TRANSACTIONS")
    print(f"[Bronze/Transactions batch {batch_id}] wrote {batch_df.count()} rows")


def write_bronze_merchants(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    write_snowflake(batch_df, "BRONZE", "MERCHANT_UPDATES")
    print(f"[Bronze/MerchantUpdates batch {batch_id}] wrote {batch_df.count()} rows")


bronze_txn_query = txn_parsed_stream.writeStream \
    .foreachBatch(write_bronze_transactions) \
    .option("checkpointLocation", BRONZE_TXN_CHECKPOINT) \
    .outputMode("append") \
    .start()

bronze_merch_query = merch_parsed_stream.writeStream \
    .foreachBatch(write_bronze_merchants) \
    .option("checkpointLocation", BRONZE_MERCH_CHECKPOINT) \
    .outputMode("append") \
    .start()


# =========================================================================
# Stage D - PII masking module
# =========================================================================

def mask_pii(df):
    return df \
        .withColumn("cc_num_hash", sha2(col("cc_num").cast("string"), 256)) \
        .withColumn("customer_first_name", regexp_replace(col("customer_first_name"), r".+", "***MASKED***")) \
        .withColumn("customer_last_name", regexp_replace(col("customer_last_name"), r".+", "***MASKED***")) \
        .withColumn("street", regexp_replace(col("street"), r".+", "***MASKED***")) \
        .withColumn("dob", regexp_replace(col("dob"), r".+", "***MASKED***")) \
        .drop("cc_num")  # drop raw card number, keep only the hash


# =========================================================================
# Stage E - Fraud detection logic
# =========================================================================
# Thresholds informed by EDA findings (fraud avg ~$531 vs legit ~$68 -
# see analysis/data_analysis.ipynb / README Key Findings).

AMOUNT_HIGH_THRESHOLD = 200.0
VELOCITY_THRESHOLD = 3        # transactions by same card within this micro-batch
RISK_SCORE_THRESHOLD = 70     # merchant risk_score considered "high risk"


def apply_fraud_detection(df):
    # Velocity: count of transactions per card within this micro-batch, as a proxy
    # for "several transactions in a short window" (micro-batches are seconds apart).
    velocity_window = Window.partitionBy("cc_num_hash")
    df = df.withColumn("txn_count_in_batch", count("transaction_id").over(velocity_window))

    df = df \
        .withColumn("high_amount_flag", when(col("amt") > AMOUNT_HIGH_THRESHOLD, 1).otherwise(0)) \
        .withColumn("high_velocity_flag", when(col("txn_count_in_batch") >= VELOCITY_THRESHOLD, 1).otherwise(0)) \
        .withColumn("high_risk_merchant_flag",
                    when(col("risk_score") >= RISK_SCORE_THRESHOLD, 1).otherwise(0)) \
        .withColumn("blacklisted_merchant_flag",
                    when(col("is_blacklisted") == True, 1).otherwise(0))

    # Combined risk score: simple weighted sum of the individual signals (0-100 scale)
    df = df.withColumn(
        "risk_score_computed",
        (col("high_amount_flag") * 25) +
        (col("high_velocity_flag") * 30) +
        (col("high_risk_merchant_flag") * 25) +
        (col("blacklisted_merchant_flag") * 20)
    )

    df = df.withColumn("fraud_flag", when(col("risk_score_computed") >= 50, True).otherwise(False))

    return df


# =========================================================================
# Stage B + C + D + E + F, combined per micro-batch via foreachBatch
# =========================================================================

SILVER_COLUMN_ORDER = [
    "transaction_id", "event_timestamp", "cc_num_hash", "merchant", "category",
    "amt", "customer_first_name", "customer_last_name", "gender", "street",
    "city", "state", "zip", "customer_lat", "customer_long", "city_pop",
    "job", "dob", "merch_lat", "merch_long", "is_fraud",
    "risk_score", "is_blacklisted", "last_flagged_date", "compliance_status", "last_updated",
    "txn_count_in_batch", "high_amount_flag", "high_velocity_flag",
    "high_risk_merchant_flag", "blacklisted_merchant_flag",
    "risk_score_computed", "fraud_flag"
]


def process_silver_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    # Stage B: load latest-per-merchant reference data fresh each batch
    merchant_bronze = read_snowflake("BRONZE", "MERCHANT_UPDATES")
    latest_window = Window.partitionBy("merchant").orderBy(col("last_updated").desc())
    merchant_ref_df = merchant_bronze \
        .withColumn("rn", row_number().over(latest_window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Stage C: enrichment join (stream-static; batch_df here is one streaming
    # micro-batch materialized as a regular DataFrame inside foreachBatch)
    enriched = batch_df.join(merchant_ref_df, on="merchant", how="left")

    # Stage D: PII masking
    masked = mask_pii(enriched)

    # Stage E: fraud detection
    flagged = apply_fraud_detection(masked)

    # NEW: force column order to exactly match the Snowflake table's DDL -
    # the Snowflake connector writes by column POSITION, not by name, so
    # a mismatched order silently sends values into the wrong columns.
    flagged = flagged.select(*SILVER_COLUMN_ORDER)

    # Stage F: write to Silver
    write_snowflake(flagged, "SILVER", "TRANSACTIONS_ENRICHED")
    print(f"[Silver batch {batch_id}] wrote {flagged.count()} rows "
          f"({flagged.filter(col('fraud_flag') == True).count()} flagged)")


silver_query = txn_parsed_stream.writeStream \
    .foreachBatch(process_silver_batch) \
    .option("checkpointLocation", SILVER_CHECKPOINT) \
    .outputMode("append") \
    .start()

print("All streaming queries started. Waiting for data...")
spark.streams.awaitAnyTermination()