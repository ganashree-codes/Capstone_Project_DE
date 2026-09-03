-- Flags any transaction with a negative amount, which should never happen.

SELECT *
FROM FINSHIELD.SILVER_GOLD.fact_transactions
WHERE amt < 0