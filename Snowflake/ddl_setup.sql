-- snowflake/ddl_setup.sql
-- Run this ONCE in a Snowflake worksheet before starting streaming_job.py.
-- Creates the database, schemas, and Bronze/Silver tables matching the
-- columns written by spark_jobs/streaming_job.py.

CREATE DATABASE IF NOT EXISTS FINSHIELD;

CREATE SCHEMA IF NOT EXISTS FINSHIELD.BRONZE;
CREATE SCHEMA IF NOT EXISTS FINSHIELD.SILVER;
CREATE SCHEMA IF NOT EXISTS FINSHIELD.GOLD;

-- =========================================================================
-- BRONZE — raw, as-received data (matches transaction_schema in streaming_job.py)
-- =========================================================================

CREATE OR REPLACE TABLE FINSHIELD.BRONZE.TRANSACTIONS (
    transaction_id        STRING,
    event_timestamp       STRING,
    cc_num                STRING,
    merchant               STRING,
    category               STRING,
    amt                     FLOAT,
    customer_first_name    STRING,
    customer_last_name     STRING,
    gender                  STRING,
    street                  STRING,
    city                    STRING,
    state                   STRING,
    zip                     STRING,
    lat                     FLOAT,
    long                    FLOAT,
    city_pop                 INTEGER,
    job                      STRING,
    dob                      STRING,
    merch_lat                FLOAT,
    merch_long                FLOAT,
    is_fraud                  INTEGER
);


-- matches merchant_schema in streaming_job.py
CREATE TABLE IF NOT EXISTS FINSHIELD.BRONZE.MERCHANT_UPDATES (
    merchant               STRING,
    risk_score              INTEGER,
    is_blacklisted           BOOLEAN,
    last_flagged_date        STRING,
    compliance_status         STRING,
    last_updated               STRING
);

-- =========================================================================
-- SILVER — masked, enriched, fraud-flagged (matches process_silver_batch output)
-- =========================================================================

CREATE OR REPLACE TABLE FINSHIELD.SILVER.TRANSACTIONS_ENRICHED (
    transaction_id            STRING,
    event_timestamp            STRING,
    cc_num_hash                 STRING,
    merchant                     STRING,
    category                     STRING,
    amt                           FLOAT,
    customer_first_name          STRING,
    customer_last_name           STRING,
    gender                        STRING,
    street                        STRING,
    city                          STRING,
    state                         STRING,
    zip                           STRING,
    lat                            FLOAT,
    long                           FLOAT,
    city_pop                       INTEGER,
    job                            STRING,
    dob                            STRING,
    merch_lat                      FLOAT,
    merch_long                      FLOAT,
    is_fraud                        INTEGER,
    risk_score                      INTEGER,
    is_blacklisted                   BOOLEAN,
    last_flagged_date                 STRING,
    compliance_status                  STRING,
    last_updated                        STRING,
    txn_count_in_batch                   INTEGER,
    high_amount_flag                      INTEGER,
    high_velocity_flag                     INTEGER,
    high_risk_merchant_flag                 INTEGER,
    blacklisted_merchant_flag                INTEGER,
    risk_score_computed                       INTEGER,
    age_group                                  STRING,
    fraud_flag                                  BOOLEAN
);