WITH source AS (
  SELECT * FROM {{ source('sales_analytics', 'xray_curves_qtd_fitted') }}
)

SELECT *
FROM
    source
ORDER BY
  source._uploaded_at
