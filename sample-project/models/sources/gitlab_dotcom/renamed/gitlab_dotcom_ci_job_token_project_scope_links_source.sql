WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_job_token_project_scope_links_dedupe_source') }}

), parsed_columns AS (

    SELECT
      id::NUMBER                  AS ci_job_token_project_scope_link_id,
      created_at::TIMESTAMP       AS created_at,
      source_project_id::NUMBER   AS source_project_id,
      target_project_id::NUMBER   AS target_project_id,
      added_by_id::NUMBER         AS added_by_id,
      direction::NUMBER           AS direction
    FROM source

)

SELECT *
FROM parsed_columns