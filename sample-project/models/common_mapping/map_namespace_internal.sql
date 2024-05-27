{{ config({
        "materialized": "view",
    })
}}

WITH final AS (

  SELECT DISTINCT 
    namespace_id AS ultimate_parent_namespace_id
  FROM {{ref('internal_gitlab_namespaces')}}
  WHERE namespace_id IS NOT NULL

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@pempey",
    created_date="2020-12-29",
    updated_date="2023-01-31"
) }}
