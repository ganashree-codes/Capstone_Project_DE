
-- Flags any transaction with a negative amount, which should never happen.

SELECT *
FROM {{ ref('fact_transactions') }}
WHERE amt < 0
