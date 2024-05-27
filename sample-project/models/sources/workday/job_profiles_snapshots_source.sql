WITH source AS (

  SELECT *
  FROM {{ source('snapshots', 'job_profiles_snapshots') }}

),

final AS (

  SELECT
    job_code::VARCHAR                       AS job_code,
    job_profile::VARCHAR                    AS job_profile,
    job_family::VARCHAR                     AS job_family,
    management_level::VARCHAR               AS management_level,
    job_level::VARCHAR                      AS job_level,
    IFF(inactive::BOOLEAN = 0, TRUE, FALSE) AS is_job_profile_active,
    dbt_valid_from::TIMESTAMP               AS valid_from,
    dbt_valid_to::TIMESTAMP                 AS valid_to,
    job_workday_id::VARCHAR                 AS job_workday_id
  FROM source

)

SELECT *
FROM final
