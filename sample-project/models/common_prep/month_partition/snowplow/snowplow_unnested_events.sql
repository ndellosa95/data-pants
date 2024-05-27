{{config({
    "materialized":"incremental",
    "unique_key":"event_id",
    "cluster_by":['event', 'derived_tstamp::DATE'],
    "on_schema_change":"sync_all_columns"
  })
}}

WITH gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events') }}
    {% if is_incremental() %}

    WHERE TRY_TO_TIMESTAMP(derived_tstamp) > (SELECT MAX(derived_tstamp) FROM {{this}})

    {% endif %}

), events_to_ignore as (

    SELECT event_id
    FROM {{ ref('snowplow_duplicate_events') }}

)

SELECT *
FROM gitlab
WHERE event_id NOT IN (SELECT event_id FROM events_to_ignore)
