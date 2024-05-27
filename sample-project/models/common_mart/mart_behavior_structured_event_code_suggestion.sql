{{ config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['mnpi_exception','product'],
    on_schema_change='sync_all_columns',
    cluster_by=['behavior_at::DATE']
  ) 

}}

{{ simple_cte([
    ('fct_behavior_structured_event_code_suggestion', 'fct_behavior_structured_event_code_suggestion'),
    ('fct_behavior_structured_event_ide_extension_version', 'fct_behavior_structured_event_ide_extension_version'),
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('dim_behavior_event', 'dim_behavior_event')
]) }}

, fct_behavior_structured_event AS (

 SELECT
  {{ dbt_utils.star(from=ref('fct_behavior_structured_event'), except=["CREATED_BY", 
    "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT",
    "IDE_EXTENSION_VERSION_CONTEXT","EXTENSION_NAME","EXTENSION_VERSION","IDE_NAME","IDE_VENDOR","IDE_VERSION","LANGUAGE_SERVER_VERSION",
    "MODEL_ENGINE","MODEL_NAME","PREFIX_LENGTH","SUFFIX_LENGTH","LANGUAGE","USER_AGENT","DELIVERY_TYPE","API_STATUS_CODE","NAMESPACE_IDS","INSTANCE_ID","HOST_NAME"]) }}
  FROM fct_behavior_structured_event
  

),

code_suggestions_context AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_behavior_structured_event_code_suggestion'), except=["CREATED_BY", 
    "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_behavior_structured_event_code_suggestion

),

ide_extension_version_context AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_behavior_structured_event_ide_extension_version'), except=["BEHAVIOR_AT", 
    "CREATED_BY", "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_behavior_structured_event_ide_extension_version

),

joined_code_suggestions_contexts AS (

  /*
  All Code Suggestions-related events have the code_suggestions_context, but only a subset
  have the ide_extension_version_context.
  */

  SELECT
    code_suggestions_context.*,
    ide_extension_version_context.ide_extension_version_context,
    ide_extension_version_context.extension_name,
    ide_extension_version_context.extension_version,
    ide_extension_version_context.ide_name,
    ide_extension_version_context.ide_vendor,
    ide_extension_version_context.ide_version,
    ide_extension_version_context.language_server_version
  FROM code_suggestions_context
  LEFT JOIN ide_extension_version_context
    ON code_suggestions_context.behavior_structured_event_pk = ide_extension_version_context.behavior_structured_event_pk
  {% if is_incremental() %}

    WHERE code_suggestions_context.behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})

  {% endif %}

),

code_suggestions_joined_to_fact_and_dim AS (

  SELECT
    joined_code_suggestions_contexts.*,
    fct_behavior_structured_event.app_id,
    fct_behavior_structured_event.contexts,
    fct_behavior_structured_event.has_code_suggestions_context,
    fct_behavior_structured_event.has_ide_extension_version_context,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_label,
    dim_behavior_event.event_property,
    CASE
      WHEN joined_code_suggestions_contexts.ide_name = 'Visual Studio Code' 
        AND joined_code_suggestions_contexts.extension_version = '3.76.0' 
        THEN TRUE --exclude IDE events from VS Code extension version 3.76.0 (which sent duplicate events)
      ELSE FALSE
    END AS is_event_to_exclude
  FROM joined_code_suggestions_contexts
  INNER JOIN fct_behavior_structured_event
    ON joined_code_suggestions_contexts.behavior_structured_event_pk = fct_behavior_structured_event.behavior_structured_event_pk
  LEFT JOIN dim_behavior_event
    ON fct_behavior_structured_event.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
  WHERE fct_behavior_structured_event.behavior_at >= '2023-08-25' --first day with events
    AND fct_behavior_structured_event.has_code_suggestions_context = TRUE

),

filtered_code_suggestion_events AS (

  SELECT
    behavior_structured_event_pk,
    behavior_at,
    behavior_at::DATE AS behavior_date,
    app_id,
    event_category,
    event_action,
    event_label,
    event_property,
    language,
    delivery_type,
    model_engine,
    model_name,
    prefix_length,
    suffix_length,
    api_status_code,
    extension_name,
    extension_version,
    ide_name,
    ide_vendor,
    ide_version,
    language_server_version,
    contexts,
    code_suggestions_context,
    ide_extension_version_context,
    has_code_suggestions_context,
    has_ide_extension_version_context,
    instance_id,
    host_name,
    is_streaming,
    gitlab_global_user_id,
    namespace_ids,
    ultimate_parent_namespace_ids,
    dim_installation_ids,
    host_names,
    subscription_names,
    dim_crm_account_ids,
    crm_account_names,
    dim_parent_crm_account_ids,
    parent_crm_account_names,
    dim_crm_account_id,
    crm_account_name,
    dim_parent_crm_account_id,
    parent_crm_account_name,
    subscription_name,
    ultimate_parent_namespace_id,
    dim_installation_id,
    installation_host_name,
    namespace_is_internal
  FROM code_suggestions_joined_to_fact_and_dim
  WHERE app_id IN ('gitlab_ai_gateway', 'gitlab_ide_extension') --"official" Code Suggestions app_ids
    AND is_event_to_exclude = FALSE --only include the good events

)

{{ dbt_audit(
    cte_ref="filtered_code_suggestion_events",
    created_by="@michellecooper",
    updated_by="@cbraza",
    created_date="2024-04-09",
    updated_date="2024-05-14"
) }}
