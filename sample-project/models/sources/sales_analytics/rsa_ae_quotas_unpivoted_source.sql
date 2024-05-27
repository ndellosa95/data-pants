WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'ae_quotas_unpivoted') }}
)

SELECT *
FROM
    source
