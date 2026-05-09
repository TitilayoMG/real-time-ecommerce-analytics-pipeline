


select distinct
    transaction,
    order_date,

    customer_id,
    product_id,

    quantity,
    price,

    quantity * price as total_amount,

    payment_method,
    order_status

from "dev"."stream_schema"."stream_store_stg"