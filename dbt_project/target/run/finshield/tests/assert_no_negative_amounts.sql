
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Flags any transaction with a negative amount, which should never happen.

SELECT *
FROM FINSHIELD.SILVER_GOLD.fact_transactions
WHERE amt < 0
  
  
      
    ) dbt_internal_test