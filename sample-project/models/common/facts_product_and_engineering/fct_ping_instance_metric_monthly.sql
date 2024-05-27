{% set filter_date = (run_started_at - modules.datetime.timedelta(days=31)).strftime('%Y-%m-%d') %}

{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    unique_key = "ping_instance_metric_id",
    on_schema_change="sync_all_columns",
    post_hook=["DELETE FROM {{ this }} WHERE is_current_ping = FALSE "]
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('dim_ping_metric', 'dim_ping_metric')
    ])

}},


fct_ping_instance_metric AS (

  SELECT
      {{ dbt_utils.star(from=ref('fct_ping_instance_metric'), except=['METRIC_VALUE', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE',
          'MODEL_UPDATED_DATE', 'DBT_CREATED_AT', 'DBT_UPDATED_AT']) }},
    TRY_TO_NUMBER(metric_value::TEXT) AS metric_value
  FROM {{ ref('fct_ping_instance_metric') }}
  {% if is_incremental() %}
    WHERE uploaded_at > '{{ filter_date }}'
  {% endif %}

),

filtered_fct_ping_instance_metric AS (
  SELECT
    fct_ping_instance_metric.*,
    dim_ping_metric.time_frame,
    dim_ping_instance.is_last_ping_of_month AS is_current_ping
  FROM fct_ping_instance_metric
  INNER JOIN dim_ping_metric
    ON fct_ping_instance_metric.metrics_path = dim_ping_metric.metrics_path
  INNER JOIN dim_ping_instance
    ON fct_ping_instance_metric.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
  WHERE dim_ping_metric.time_frame IN ('28d', 'all')
    {% if is_incremental() %}
    AND dim_ping_instance.next_ping_uploaded_at > '{{ filter_date }}'
    {% else %}
    -- Only filtered on a full refresh as the post_hook DELETE step applies the filter during an incremental build
    AND dim_ping_instance.is_last_ping_of_month = TRUE
    {% endif %}
    AND fct_ping_instance_metric.has_timed_out = FALSE
    AND fct_ping_instance_metric.metric_value IS NOT NULL
),

time_frame_28_day_metrics AS (

  SELECT
    *,
    metric_value AS monthly_metric_value
  FROM filtered_fct_ping_instance_metric
  WHERE time_frame = ('28d')

),

time_frame_all_time_metrics AS (

  SELECT
    *,
      {{ monthly_all_time_metric_calc('metric_value', 'dim_installation_id',
                                    'metrics_path', 'ping_created_at') }}
  FROM filtered_fct_ping_instance_metric
  WHERE time_frame = ('all')

),

final AS (

  SELECT
    ping_instance_metric_id,
    dim_ping_instance_id,
    dim_product_tier_id,
    dim_subscription_id,
    dim_location_country_id,
    dim_ping_date_id,
    dim_instance_id,
    dim_host_id,
    dim_installation_id,
    dim_license_id,
    dim_subscription_license_id,
    dim_app_release_major_minor_sk,
    dim_latest_available_app_release_major_minor_sk,
    metrics_path,
    metric_value,
    monthly_metric_value,
    time_frame,
    has_timed_out,
    ping_created_at,
    umau_value,
    data_source,
    uploaded_at,
    is_current_ping
  FROM time_frame_28_day_metrics

  UNION ALL

  SELECT
    ping_instance_metric_id,
    dim_ping_instance_id,
    dim_product_tier_id,
    dim_subscription_id,
    dim_location_country_id,
    dim_ping_date_id,
    dim_instance_id,
    dim_host_id,
    dim_installation_id,
    dim_license_id,
    dim_subscription_license_id,
    dim_app_release_major_minor_sk,
    dim_latest_available_app_release_major_minor_sk,
    metrics_path,
    metric_value,
    IFF(monthly_metric_value < 0, 0, monthly_metric_value) AS monthly_metric_value,
    time_frame,
    has_timed_out,
    ping_created_at,
    umau_value,
    data_source,
    uploaded_at,
    is_current_ping
  FROM time_frame_all_time_metrics

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@pempey",
    created_date="2022-05-09",
    updated_date="2024-05-09"
) }}
