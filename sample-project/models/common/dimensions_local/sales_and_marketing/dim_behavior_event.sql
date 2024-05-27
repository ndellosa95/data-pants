{{ config(
        materialized = "incremental",
        unique_key = "dim_behavior_event_sk",
        tags=['product']
) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unnested_events_all')
    ])
}}

, final AS (

    SELECT
      dim_behavior_event_sk,
      event,
      event_name,
      platform,
      environment,
      event_category,
      event_action,
      event_label,
      clean_event_label,
      event_property,
      MAX(behavior_at)   AS max_timestamp
    FROM events

    {% if is_incremental() %}
    
    WHERE behavior_at > (SELECT MAX(max_timestamp) FROM {{this}})
    
    {% endif %}

    {{ dbt_utils.group_by(n=10) }}
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@chrissharp",
    updated_by="@utkarsh060",
    created_date="2022-09-20",
    updated_date="2024-05-08"
) }}
