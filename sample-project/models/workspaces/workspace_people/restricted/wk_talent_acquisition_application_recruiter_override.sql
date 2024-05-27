WITH greenhouse_applications_jobs_source AS (
  SELECT *
  FROM {{ ref('greenhouse_applications_jobs_source') }}
),

greenhouse_candidates_source AS (
  SELECT *
  FROM {{ ref('greenhouse_candidates_source') }}
),

greenhouse_job_custom_fields_source AS (
  SELECT *
  FROM {{ ref('greenhouse_job_custom_fields_source') }}
),

greenhouse_applications_source AS (
  SELECT *
  FROM {{ ref('greenhouse_applications_source') }}
),

xf AS (
  SELECT
    xf.application_id,
    xf.job_id,
    ROW_NUMBER() OVER (
      PARTITION BY xf.application_id ORDER BY xf.job_id DESC
    ) AS job_id_rank
  FROM greenhouse_applications_jobs_source AS xf
),

cand AS (
  SELECT
    candidate_id,
    candidate_recruiter
  FROM greenhouse_candidates_source
),

job_custom AS (
  SELECT
    job_id AS job_custom_id,
    MAX(CASE job_custom_field
      WHEN 'Job Grade'
        THEN job_custom_field_display_value
    END)   AS job_grade
  FROM greenhouse_job_custom_fields_source
  GROUP BY 1
),

app AS (
  SELECT
    app.application_id,
    app.candidate_id,
    xf.job_id,
    cand.candidate_recruiter                                                      AS app_recruiter,
    --,job_grade
    IFF(job_custom.job_grade IN ('10', '11', '12', '13', '14', '15'), 'Dir+', '') AS app_recruiter_map_override
  FROM greenhouse_applications_source AS app
  INNER JOIN xf
    ON app.application_id = xf.application_id
      AND 1 = xf.job_id_rank
  LEFT JOIN cand ON app.candidate_id = cand.candidate_id
  LEFT JOIN job_custom ON xf.job_id = job_custom.job_custom_id
)

SELECT *
FROM app
