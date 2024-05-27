WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'ae_credits') }}
)

SELECT *
FROM
    source