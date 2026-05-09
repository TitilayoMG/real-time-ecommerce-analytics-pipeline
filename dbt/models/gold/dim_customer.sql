
{{ config(materialized='table') }}


select distinct
    customer_id,
    customer_name,
    country
from {{ ref('stream_store_stg') }}








