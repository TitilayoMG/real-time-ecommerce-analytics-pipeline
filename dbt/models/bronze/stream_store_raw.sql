{{ config(materialized='view', tags=['bronze']) }}


SELECT *
FROM public.stream_store_raw