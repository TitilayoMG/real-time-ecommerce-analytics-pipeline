
  
    

  create  table
    "dev"."stream_schema"."dim_customer__dbt_tmp"
    
    
    
  as (
    


select distinct
    customer_id,
    customer_name,
    country
from "dev"."stream_schema"."stream_store_stg"
  );
  