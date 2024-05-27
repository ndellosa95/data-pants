{{
    config(
        materialized='table'
    )
}}

WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'tableau_asm_consolidated_sources') }}
)

SELECT *
FROM source