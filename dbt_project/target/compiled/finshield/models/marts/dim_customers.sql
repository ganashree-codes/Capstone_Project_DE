SELECT DISTINCT
    cc_num_hash AS customer_id,
    customer_first_name,
    customer_last_name,
    gender,
    state,
    city,
    zip,
    job,
    age_group
FROM FINSHIELD.SILVER_SILVER.stg_transactions
WHERE cc_num_hash IS NOT NULL