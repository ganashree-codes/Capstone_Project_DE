--Bronze data analysis
--count number of rows
select count(*) from bronze.transactions

select count(*) from bronze.merchant_updates

--null counts per column
select count(*) as null_count from bronze.transactions where transaction_id is null

select count(*) as null_count from bronze.transactions where merchant is null

select count(*) as null_count from bronze.transactions where amt is null

--find duplicate counts

FINSHIELD.BRONZEFINSHIELD.GOLDFINSHIELD.GOLDSELECT transaction_id,COUNT(*) AS duplicate_count
FROM bronze.transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

--total transactions

select count(transaction_id) from transactions

--total fraud flagged

select count(is_fraud) from transactions where is_fraud = 1

-- find fraud rate %
SELECT
    category,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY category
ORDER BY fraud_rate_pct DESC;

--top 10 customers by total spent

SELECT CONCAT(customer_first_name || ' ' || customer_last_name) as Customer_full_name, SUM(amt) as total_spend
from bronze.transactions
GROUP BY Customer_full_name
ORDER BY total_spend DESC
LIMIT 10

--top 10 average transaction amount

SELECT 
    merchant, 
    AVG(amt) AS avg_transaction_amount,
    COUNT(transaction_id) AS total_transactions
FROM bronze.transactions
GROUP BY merchant
ORDER BY avg_transaction_amount DESC
LIMIT 10;

-- find merchants whose risk score is greater than 70

SELECT merchant, risk_score
from bronze.merchant_updates
where risk_score > 70

--find merchants who are in the blacklist

select merchant, is_blacklisted
from bronze.merchant_updates
where is_blacklisted = TRUE


--==========================================================================================================================
--Silver data Analysis
select count(*) from silver.transactions_enriched

select * from silver.transactions_enriched

select count(*) as null_count from silver.transactions_enriched where transaction_id is null

--compute the percentage of how much did my system flagged as suspisious--
SELECT
    ROUND(SUM(CASE WHEN fraud_flag THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS my_detection_flag_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED;

--check if fraud_flag correctly detected is_fraud by comparing--

SELECT
    SUM(CASE WHEN fraud_flag = TRUE AND is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN fraud_flag = TRUE AND is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN fraud_flag = FALSE AND is_fraud = 1 THEN 1 ELSE 0 END) AS false_negatives,
    SUM(CASE WHEN fraud_flag = FALSE AND is_fraud = 0 THEN 1 ELSE 0 END) AS true_negatives
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED;


--top 10 merchants by transaction 

SELECT 
    merchant, 
    COUNT(transaction_id) AS transaction_count
FROM silver.transactions_enriched
GROUP BY merchant
ORDER BY transaction_count DESC
LIMIT 10;

--top 10 merchants by fraud count

SELECT 
    merchant, 
    COUNT(is_fraud) AS fraud_count
FROM silver.transactions_enriched
WHERE is_fraud = 1
GROUP BY merchant
ORDER BY fraud_count DESC
LIMIT 10;

--top 10 merchants based on flagged fraud
SELECT 
    merchant, 
    COUNT(fraud_flag) AS fraud_count
FROM silver.transactions_enriched
WHERE fraud_flag = TRUE
GROUP BY merchant
ORDER BY fraud_count DESC
LIMIT 10;

-- filter merchants based on compilance status
SELECT 
    merchant, 
    category, 
    compliance_status,
FROM silver.transactions_enriched
GROUP BY merchant, category, compliance_status
ORDER BY compliance_status, category;


--state level fraud iterate

SELECT
    state,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY state
HAVING COUNT(*) >= 100   -- avoid small-sample noise, same lesson as your merchant/job EDA
ORDER BY fraud_rate_pct DESC
LIMIT 10;

--find the fraud pattern based on time of the day whether day or night where most fraud happens
SELECT
    CASE
        WHEN HOUR(TO_TIMESTAMP(event_timestamp)) BETWEEN 6 AND 21 THEN 'Day (6am-10pm)'
        ELSE 'Night (10pm-6am)'
    END AS time_period,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY time_period
ORDER BY fraud_rate_pct DESC;

--weekend vs weekday fraud pattern 
SELECT
    CASE
        WHEN DAYOFWEEK(TO_TIMESTAMP(event_timestamp)) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY day_type;

--hour by hour fraud rate 
SELECT
    HOUR(TO_TIMESTAMP(event_timestamp)) AS hour_of_day,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY hour_of_day
ORDER BY hour_of_day;

--merchant fraud rate
SELECT
    merchant,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY merchant
HAVING COUNT(*) >= 30
ORDER BY fraud_rate_pct DESC
LIMIT 10;

--merchant risk score vs actual fraud rate

SELECT
    CASE
        WHEN risk_score < 25 THEN 'Low (0-24)'
        WHEN risk_score < 50 THEN 'Medium (25-49)'
        WHEN risk_score < 75 THEN 'High (50-74)'
        ELSE 'Very High (75-100)'
    END AS risk_bucket,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
WHERE risk_score IS NOT NULL
GROUP BY risk_bucket
ORDER BY risk_bucket;

--amount distribution by fraud status

SELECT
    is_fraud,
    COUNT(*) AS txn_count,
    ROUND(AVG(amt), 2) AS avg_amount,
    ROUND(MEDIAN(amt), 2) AS median_amount,
    ROUND(MAX(amt), 2) AS max_amount
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY is_fraud;

--gender based fraud iterate
SELECT
    gender,
    COUNT(*) AS total_transactions,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY gender;

--which combination of signals correlates most with real fraud
SELECT
    high_amount_flag,
    high_velocity_flag,
    high_risk_merchant_flag,
    blacklisted_merchant_flag,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY high_amount_flag, high_velocity_flag, high_risk_merchant_flag, blacklisted_merchant_flag
ORDER BY fraud_rate_pct DESC;


--find fraud rate based on age group 
SELECT
    age_group,
    COUNT(*) AS txn_count,
    SUM(is_fraud) AS fraud_count,
    ROUND(SUM(is_fraud) / COUNT(*) * 100, 2) AS fraud_rate_pct
FROM FINSHIELD.SILVER.TRANSACTIONS_ENRICHED
GROUP BY age_group
ORDER BY fraud_rate_pct DESC;

