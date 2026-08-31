# Data Provenance

## Source Datasets

**Transactions**: [Credit Card Transactions Fraud Detection Dataset](https://www.kaggle.com/datasets/kartik2112/fraud-detection)
(Kaggle — simulated but realistic transaction data, includes ground-truth `is_fraud` label)
- `fraudTrain.csv` — 1,296,675 rows — used to design/tune fraud detection logic and as the primary stream for the live producer/demo pipeline.
- `fraudTest.csv` — 555,719 rows — held out, unseen data used only for final precision/recall validation of the detection logic (not used while designing rules).
- Verified during EDA: 0 duplicate rows, 0 null values across all columns in both files.

**Merchant risk data**: synthetically generated via Python `Faker`, streamed through NiFi. Not from the Kaggle dataset — represents a simulated compliance/risk-scoring feed (risk_score, blacklist status) that doesn't exist in the raw transaction data.

## What's Real vs. Simulated

- Transaction records and the `is_fraud` label are real, dataset-provided historical data.
- Streaming is **simulated**: the static CSV is replayed through a Kafka producer at a configurable interval to mimic a live transaction feed. From Kafka onward (Spark Structured Streaming, enrichment, detection, writes to Snowflake), the processing architecture is genuine streaming.
- Merchant risk scores are entirely synthetic (Faker-generated), designed to represent realistic reference/compliance data that a production system would maintain independently of transaction data.
- This system detects and flags suspicious transactions for review; it does not perform real-time transaction authorization/blocking, which is a separate system in production fraud architectures.

## Canonical Event Schema (Transactions)

Raw CSV columns renamed/dropped once during producer prep, then used consistently across Kafka, Spark, and Snowflake:

| Original CSV column | Canonical field name | Type | Notes |
|---|---|---|---|
| `Unnamed: 0` | *(dropped)* | — | Redundant row-index artifact from CSV export, no real information |
| `trans_num` | `transaction_id` | string | True unique transaction identifier |
| `trans_date_trans_time` | `event_timestamp` | string (ISO8601) | Renamed for clarity |
| `cc_num` | `cc_num` | string | PII — masked/hashed (SHA-256) downstream in Spark before Silver |
| `merchant` | `merchant` | string | Join key against merchant risk feed |
| `category` | `category` | string | Merchant category |
| `amt` | `amt` | float | Transaction amount |
| `first` | `customer_first_name` | string | PII — masked downstream |
| `last` | `customer_last_name` | string | PII — masked downstream |
| `gender` | `gender` | string | |
| `street`, `city`, `state`, `zip` | *(same names)* | string | PII (address) — masked downstream |
| `lat`, `long` | `customer_lat`, `customer_long` | float | Customer home coordinates |
| `city_pop` | `city_pop` | int | |
| `job` | `job` | string | |
| `dob` | `dob` | string (date) | PII — masked downstream |
| `merch_lat`, `merch_long` | `merch_lat`, `merch_long` | float | Merchant location — used for geo-distance fraud signal |
| `is_fraud` | `is_fraud` | int (0/1) | Ground truth — used only for offline precision/recall validation, never as an input to the detection logic itself |

## Canonical Schema (Merchant Risk Feed — via NiFi)

| Field | Type | Notes |
|---|---|---|
| `merchant` | string | Must exactly match the `merchant` field name/values in the transactions schema — this is the join key in Spark |
| `risk_score` | int (0-100) | Faker-generated |
| `is_blacklisted` | boolean | Faker-generated |
| `last_flagged_date` | string (date) | Faker-generated |
| `compliance_status` | string | Faker-generated |
