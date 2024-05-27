{{
    config(
        materialized='table'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'input_state_to_region') }}
)

SELECT *
FROM source