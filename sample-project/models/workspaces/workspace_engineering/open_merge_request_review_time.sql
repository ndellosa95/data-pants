WITH product_mrs AS (

  SELECT * EXCLUDE (merge_request_description)
  FROM {{ ref('engineering_merge_requests') }}
  WHERE created_at >= DATEADD(MONTH, -36, CURRENT_DATE)

),

map_employee_info AS (

  SELECT
    a.*,
    b.user_id
  FROM {{ ref('mart_team_member_directory') }} AS a
  INNER JOIN {{ ref('map_team_member_user') }} AS b ON a.employee_id = b.employee_id

),

/*when mr author reassigns to someone other than themself, we recognize this as a review request*/
first_non_author_assignment AS (

  SELECT
    mrs.merge_request_id,
    0                    AS review_requests,
    0                    AS reviewer_count,
    MIN(note_created_at) AS first_review_date
  FROM product_mrs AS mrs
  INNER JOIN {{ ref('gitlab_dotcom_merge_request_assignment_events') }} AS asevs ON mrs.merge_request_id = asevs.merge_request_id AND mrs.author_id != asevs.event_user_id
  WHERE asevs.event IN ('assigned', 'reassigned')
  GROUP BY 1

),

/*retrieve review dates from notes table*/
notes AS (

  SELECT
    created_at                                           AS review_requested_at,
    noteable_id                                          AS merge_request_id,
    note,
    MAX(created_at) OVER (PARTITION BY merge_request_id) AS last_note_date
  FROM {{ ref('internal_notes') }} AS notes
  WHERE notes.noteable_type = 'MergeRequest'
--     and array_contains('reviewer'::variant, notes.action_type_array)

),

extracted_usernames AS (
  SELECT
    review_requested_at,
    merge_request_id,
    REPLACE(REGEXP_SUBSTR(TRIM(value), '@([^\\s,]+)'), '@', '') AS username,
    last_note_date
  FROM notes,
    LATERAL SPLIT_TO_TABLE(note, ',')
  WHERE notes.note LIKE '%requested review from%'

),

agg AS (

  SELECT
    mrs.merge_request_id,
    GREATEST(COALESCE(COUNT(DISTINCT username), 0), COALESCE(MAX(first_non_author_assignment.reviewer_count), 0))                         AS reviewer_count,
    GREATEST(COALESCE(COUNT(DISTINCT review_requested_at || username), 0), COALESCE(MAX(first_non_author_assignment.review_requests), 0)) AS review_requests,
    LEAST(
      COALESCE(MIN(extracted_usernames.review_requested_at), '9999-12-31'), COALESCE(MIN(first_non_author_assignment.first_review_date), '9999-12-31'),
      COALESCE(MIN(reviewer_requested_at_creation.created_at), '9999-12-31')
    )                                                                                                                                     AS first_review_date,
    GREATEST(MAX(extracted_usernames.last_note_date), MAX(mrs.merged_at))                                                                 AS last_reported_date
  FROM product_mrs AS mrs
  LEFT JOIN extracted_usernames ON mrs.merge_request_id = extracted_usernames.merge_request_id
  LEFT JOIN first_non_author_assignment ON mrs.merge_request_id = first_non_author_assignment.merge_request_id
  LEFT JOIN {{ ref('gitlab_dotcom_merge_request_reviewers') }} AS reviewer_requested_at_creation ON mrs.merge_request_id = reviewer_requested_at_creation.merge_request_id
  GROUP BY 1

),

first_review_date AS (

  SELECT DISTINCT
    mrs.*,
    reviewer_count,
    review_requests,
    first_review_date,
    last_reported_date
  FROM product_mrs AS mrs
  INNER JOIN agg ON mrs.merge_request_id = agg.merge_request_id
  WHERE (merge_request_state = 'opened' AND mrs.merged_at IS NULL)
    OR (merge_request_state != 'opened' AND last_reported_date IS NOT NULL)

),

date_spine AS (

  SELECT date_actual
  FROM {{ ref('dim_date') }}
  WHERE date_actual BETWEEN DATEADD(MONTH, -36, CURRENT_DATE) AND CURRENT_DATE

),

/*add flag for mrs older than 365 days*/
add_old_flag AS (

  SELECT
    date_actual,
    first_review_date.*,
    CASE WHEN DATEADD('day', -365, date_actual) >= created_at AND merged_at IS NULL THEN 1 ELSE 0 END AS old_1yr_flag,
    DATEDIFF('day', created_at, date_spine.date_actual)                                               AS days_open,
    ROUND(DATEDIFF('day', created_at, first_review_date), 2)                                          AS days_to_review,
    ROUND(DATEDIFF('day', first_review_date, date_spine.date_actual), 2)                              AS days_in_review,
    PERCENT_RANK() OVER (PARTITION BY date_actual ORDER BY days_open)                                 AS days_open_p95
  FROM date_spine
  INNER JOIN first_review_date ON date_spine.date_actual BETWEEN DATE_TRUNC('day', first_review_date.first_review_date) AND
    COALESCE(merged_at, last_reported_date, CURRENT_DATE)::DATE

),

final AS (
  SELECT DISTINCT
    mr.*,
    TRY_CAST(sizes.product_merge_request_files_changed AS INTEGER) AS files_changed,
    TRY_CAST(sizes.product_merge_request_lines_added AS INTEGER)   AS added_lines,
    TRY_CAST(sizes.product_merge_request_lines_removed AS INTEGER) AS removed_lines,
    map_employee_info.gitlab_username                              AS author_gitlab_username,
    map_employee_info.division                                     AS author_division,
    map_employee_info.department                                   AS author_department,
    map_employee_info.position                                     AS author_position,
    map_employee_info.job_specialty_single                         AS author_job_specialty
  FROM add_old_flag AS mr
  LEFT JOIN {{ ref('sizes_part_of_product_merge_requests') }} AS sizes ON mr.merge_request_iid = sizes.product_merge_request_iid AND mr.target_project_id = sizes.product_merge_request_project_id
  LEFT JOIN map_employee_info ON mr.author_id = map_employee_info.user_id AND DATE_TRUNC('day', mr.created_at) BETWEEN map_employee_info.valid_from AND DATEADD('day', -1, map_employee_info.valid_to)
  WHERE mr.merge_request_id NOT IN (SELECT deleted_merge_request_id FROM {{ ref('sheetload_deleted_mrs') }})
--AND old_1yr_flag = 0
-- and date_actual = dateadd('day',-1,current_date)
-- and reviewer_count is null
)

SELECT *
FROM final
