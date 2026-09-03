SELECT
    transaction_id,
    event_timestamp,
    cc_num_hash AS customer_id,
    merchant AS merchant_id,
    amt,
    is_fraud,
    fraud_flag,
    risk_score_computed,
    high_amount_flag,
    high_velocity_flag,
    high_risk_merchant_flag,
    blacklisted_merchant_flag
FROM {{ ref('stg_transactions') }}
WHERE transaction_id IS NOT NULL
