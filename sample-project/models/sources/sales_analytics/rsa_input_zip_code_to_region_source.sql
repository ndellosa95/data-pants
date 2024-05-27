{{
    config(
        materialized='table'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'input_zip_code_to_region') }}
)

SELECT *
FROM source