-- models/marts/dim_merchant.sql

WITH ranked_merchants AS (
    SELECT
        merchant AS merchant_id,
        category AS merchant_category,
        risk_score,
        is_blacklisted,
        compliance_status,
        ROW_NUMBER() OVER (
            PARTITION BY merchant
            ORDER BY event_timestamp DESC
        ) AS rn
    FROM {{ ref('stg_transactions') }}
    WHERE merchant IS NOT NULL
)

SELECT
    merchant_id,
    merchant_category,
    risk_score,
    is_blacklisted,
    compliance_status
FROM ranked_merchants
WHERE rn = 1