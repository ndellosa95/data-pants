WITH greenhouse_offers_source AS (
  SELECT *
  FROM {{ ref('greenhouse_offers_source') }}
),

offers_source AS (
  SELECT
    offer_id,
    application_id,
    offer_status,
    created_by,
    start_date,
    created_at,
    sent_at,
    resolved_at,
    updated_at
  FROM greenhouse_offers_source
)

SELECT *
FROM offers_source
WHERE sent_at IS NOT NULL
--AND resolved_at IS NOT NULL
