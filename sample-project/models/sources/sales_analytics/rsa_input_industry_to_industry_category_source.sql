{{
    config(
        materialized='table'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'input_industry_to_industry_category') }}
)

SELECT *
FROM source