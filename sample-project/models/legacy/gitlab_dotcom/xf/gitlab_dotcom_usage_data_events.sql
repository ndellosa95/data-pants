{{ config(
    tags=["mnpi_exception"]
) }}

{{config({
    "materialized":"view",
    "alias": "gitlab_dotcom_usage_data_events"
    })
  }}
 

  SELECT *
  FROM {{ ref('gitlab_dotcom_usage_data_pipelines') }}

  UNION ALL

  SELECT *
  FROM {{ ref('gitlab_dotcom_usage_data_issues') }}

  UNION ALL

  SELECT *
  FROM {{ ref('gitlab_dotcom_usage_data_notes') }}

  UNION ALL

  SELECT *
  FROM {{ ref('gitlab_dotcom_usage_data_ci_builds') }}