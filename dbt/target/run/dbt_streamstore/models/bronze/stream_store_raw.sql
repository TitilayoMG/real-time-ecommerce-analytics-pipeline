

  create view "dev"."stream_schema"."stream_store_raw__dbt_tmp" as (
    


SELECT *
FROM public.stream_store_raw
  ) ;
