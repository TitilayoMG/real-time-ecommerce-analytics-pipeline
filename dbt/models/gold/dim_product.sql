{{ config(materialized='table') }}


select distinct
    product_id,
    product_category
from {{ ref('stream_store_stg') }}