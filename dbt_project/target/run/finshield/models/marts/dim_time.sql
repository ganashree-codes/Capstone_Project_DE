
  
    

create or replace transient table FINSHIELD.SILVER_GOLD.dim_time
    
    
    
    
    

    as (SELECT DISTINCT
    event_timestamp,
    DATE(TO_TIMESTAMP(event_timestamp)) AS date,
    HOUR(TO_TIMESTAMP(event_timestamp)) AS hour_of_day,
    DAYOFWEEK(TO_TIMESTAMP(event_timestamp)) AS day_of_week,
    CASE
        WHEN DAYOFWEEK(TO_TIMESTAMP(event_timestamp)) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS is_weekend,
    CASE
        WHEN HOUR(TO_TIMESTAMP(event_timestamp)) BETWEEN 6 AND 21 THEN 'Day'
        ELSE 'Night'
    END AS time_period
FROM FINSHIELD.SILVER_SILVER.stg_transactions
WHERE event_timestamp IS NOT NULL
    )
;


  