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
FROM {{ ref('stg_transactions') }}
WHERE cc_num_hash IS NOT NULL
