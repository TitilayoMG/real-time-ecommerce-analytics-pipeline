
  
    

  create  table
    "dev"."stream_schema"."dim_product__dbt_tmp"
    
    
    
  as (
    


select distinct
    product_id,
    product_category
from "dev"."stream_schema"."stream_store_stg"
  );
  