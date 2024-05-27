WITH metrics AS (
  SELECT
    *
  FROM {{ ref('dim_ping_metric') }}
),

final AS (
  SELECT
    metrics.metrics_path,
    COALESCE(
      TRIM(options_events.value, '"'),
      TRIM(events.value['name'], '"'),
      TRIM(data_by_row['options']['event'], '"')
    ) AS redis_event,
    metrics.metrics_status,
    metrics.time_frame,
    metrics.data_source,
    metrics.is_health_score_metric
  FROM metrics
  LEFT JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(data_by_row['options']['events']), OUTER => TRUE) AS options_events
  LEFT JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(data_by_row['events']), OUTER => TRUE) AS events
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-12-02",
    updated_date="2024-05-22"
) }}
