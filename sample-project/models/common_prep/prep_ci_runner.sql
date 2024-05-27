{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_ci_runner_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('sheetload_ci_runner_machine_type_mapping_source', 'sheetload_ci_runner_machine_type_mapping_source')

]) }}

, gitlab_dotcom_ci_runners_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_runners_source')}}
    {% if is_incremental() %}

      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), final AS (
  
    SELECT
      runner_id AS dim_ci_runner_id, 
      
      -- FOREIGN KEYS
      dim_date.date_id                                                                          AS created_date_id,         

      gitlab_dotcom_ci_runners_source.created_at,
      gitlab_dotcom_ci_runners_source.updated_at,
      gitlab_dotcom_ci_runners_source.description                                               AS ci_runner_description,
      gitlab_dotcom_ci_runners_source.contacted_at,
      gitlab_dotcom_ci_runners_source.is_active,
      gitlab_dotcom_ci_runners_source.version                                                   AS ci_runner_version,
      gitlab_dotcom_ci_runners_source.revision,
      gitlab_dotcom_ci_runners_source.platform,
      gitlab_dotcom_ci_runners_source.is_untagged,
      gitlab_dotcom_ci_runners_source.is_locked,
      gitlab_dotcom_ci_runners_source.access_level,
      gitlab_dotcom_ci_runners_source.maximum_timeout,
      gitlab_dotcom_ci_runners_source.runner_type,
      gitlab_dotcom_ci_runners_source.public_projects_minutes_cost_factor,
      gitlab_dotcom_ci_runners_source.private_projects_minutes_cost_factor,
      COALESCE(sheetload_ci_runner_machine_type_mapping_source.ci_runner_machine_type, 'Other') AS ci_runner_machine_type,
      COALESCE(sheetload_ci_runner_machine_type_mapping_source.cost_factor, 0) AS cost_factor

    FROM gitlab_dotcom_ci_runners_source
    LEFT JOIN dim_date 
      ON TO_DATE(gitlab_dotcom_ci_runners_source.created_at) = dim_date.date_day
    LEFT JOIN sheetload_ci_runner_machine_type_mapping_source
      ON gitlab_dotcom_ci_runners_source.description LIKE sheetload_ci_runner_machine_type_mapping_source.ci_runner_description_mapping

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@michellecooper",
    created_date="2021-06-23",
    updated_date="2024-05-20"
) }}
