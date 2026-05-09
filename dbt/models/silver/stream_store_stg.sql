{{ config(materialized='table') }}

WITH cleaned AS (

    SELECT
        transaction,
        CAST(order_date AS DATE) AS order_date,
        CAST(customer_id AS VARCHAR(10)) AS customer_id,
        customer_name,

        CASE
            WHEN country IN ('US', 'UK', 'Canada', 'France', 'Denmark', 'Australia')
                THEN country
            ELSE 'unknown'
        END AS country,

        CAST(product_id AS VARCHAR(10)) AS product_id,

        product_category,

        CASE
            WHEN quantity > 0 THEN quantity
            ELSE 1
        END AS quantity,

        price,
        payment_method,
        order_status

    FROM {{ ref('stream_store_raw') }}

    WHERE customer_id IS NOT NULL
      AND order_date::DATE <= CURRENT_DATE

),

deduplicated AS (

    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY transaction
               ORDER BY order_date DESC
           ) AS rn

    FROM cleaned
)

SELECT *
FROM deduplicated
WHERE rn = 1