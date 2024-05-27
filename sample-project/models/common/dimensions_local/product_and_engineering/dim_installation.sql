{{ simple_cte([('prep_host', 'prep_host'),
('prep_ping_instance', 'prep_ping_instance'),
('prep_ping_instance_flattened', 'prep_ping_instance_flattened')])}},

installation_agg AS (
  SELECT
    dim_installation_id,
    MIN(TRY_TO_TIMESTAMP(TRIM(metric_value::VARCHAR,'UTC'))) AS installation_creation_date
  FROM prep_ping_instance_flattened
  WHERE ping_created_at > '2023-03-15' --filtering out records that came before GitLab v15.10, when metric was released. Filter in place for full refresh runs.
    AND metrics_path IN (
      'installation_creation_date_approximation',
      'installation_creation_date'
    )
    AND metric_value != 0 -- 0, when cast to timestamp, returns 1970-01-01

    -- filtering out these two installations per this discussion: 
    -- https://gitlab.com/gitlab-data/analytics/-/merge_requests/8416#note_1473684265
    AND dim_installation_id NOT IN (
      'cc89cba853f6caf1a100259b1048704f',
      '5f03898242847ecbaff5b5f8973d5910'
    )
  {{ dbt_utils.group_by(n = 1) }}
),

joined AS (

  SELECT DISTINCT
    -- Primary Key
    prep_ping_instance.dim_installation_id,

    -- Natural Keys 
    prep_ping_instance.dim_instance_id,
    prep_ping_instance.dim_host_id,

    -- Dimensional contexts  
    prep_host.host_name,
    installation_agg.installation_creation_date
  FROM prep_ping_instance
  INNER JOIN prep_host ON prep_ping_instance.dim_host_id = prep_host.dim_host_id
  LEFT JOIN installation_agg ON installation_agg.dim_installation_id = prep_ping_instance.dim_installation_id

  UNION ALL

  SELECT
    MD5('-1')                 AS dim_installation_id,
    'Missing dim_instance_id' AS dim_instance_id,
    NULL                      AS dim_host_id,
    'Missing host_name'       AS host_name,
    NULL                      AS installation_creation_date
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@mdrussell",
    created_date="2021-05-20",
    updated_date="2023-07-10"
) }}
