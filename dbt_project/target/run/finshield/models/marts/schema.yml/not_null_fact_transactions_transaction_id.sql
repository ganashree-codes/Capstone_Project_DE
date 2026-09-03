
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_id
from FINSHIELD.SILVER_GOLD.fact_transactions
where transaction_id is null



  
  
      
    ) dbt_internal_test