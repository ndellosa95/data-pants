{{
    config(
        materialized='table'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'input_fy25_planning_named') }}
)

SELECT *
FROM source