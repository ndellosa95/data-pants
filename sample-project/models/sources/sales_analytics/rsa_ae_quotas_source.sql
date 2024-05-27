WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'ae_quotas') }}
)

SELECT *
FROM
    source
