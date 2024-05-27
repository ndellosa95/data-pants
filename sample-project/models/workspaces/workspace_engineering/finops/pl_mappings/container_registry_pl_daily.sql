WITH ns_type AS (

  SELECT * FROM {{ ref ('namespace_pl_daily') }}

),

projects AS (SELECT * FROM {{ ref('gitlab_dotcom_projects_xf') }}
),

project_statistics AS (

  SELECT * FROM {{ ref('gitlab_dotcom_project_statistic_daily_snapshot') }}
),

namespaces_child AS (

  SELECT * FROM {{ ref('gitlab_dotcom_namespaces_xf') }}

),

storage AS (
  SELECT
    project_statistics.snapshot_day,
    namespaces_child.namespace_ultimate_parent_id                               AS namespace_id,
    SUM(COALESCE(project_statistics.container_registry_size, 0) / POW(1024, 3)) AS container_registry_gb
  FROM
    projects
  LEFT JOIN
    project_statistics
    ON
      projects.project_id = project_statistics.project_id
  INNER JOIN
    namespaces_child
    ON
      projects.namespace_id = namespaces_child.namespace_id
  GROUP BY
    1, 2
)



SELECT
  storage.snapshot_day,
  COALESCE(ns_type.finance_pl, 'internal')                                     AS finance_pl,
  SUM(container_registry_gb)                                                   AS container_registry_gb,
  RATIO_TO_REPORT(SUM(container_registry_gb)) OVER (PARTITION BY snapshot_day) AS percent_container_registry_size
FROM
  storage
LEFT JOIN
  ns_type
  ON
    storage.namespace_id = ns_type.dim_namespace_id
    AND
    storage.snapshot_day = ns_type.date_day
GROUP BY
  1, 2
ORDER BY
  1
