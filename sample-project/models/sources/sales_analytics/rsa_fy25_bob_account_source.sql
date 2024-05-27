{{
    config(
        materialized='table'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'fy25_bob_account') }}
)

SELECT *
FROM source