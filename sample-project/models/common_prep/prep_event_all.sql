{{ config(
    tags=["product"],
    materialized = "incremental",
    unique_key = "event_pk",
    on_schema_change = "sync_all_columns"
) }}

-- depends_on: {{ ref('prep_event') }}

{% if is_incremental() %}

SELECT *
FROM {{ ref('prep_event') }}
WHERE event_created_at >= (SELECT MAX(event_created_at) FROM {{ this }})

{% else %}

WITH unioned_table AS (

{{ schema_union_all('dotcom_usage_events_', 'prep_event', database_name=env_var('SNOWFLAKE_PREP_DATABASE')) }}

)

SELECT *
FROM unioned_table
-- Some past events may change in the source system and they need to be filtered out of
-- the static month partitions
QUALIFY ROW_NUMBER() OVER (PARTITION BY event_pk ORDER BY event_created_at DESC) = 1

{% endif %}
