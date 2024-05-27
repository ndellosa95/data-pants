WITH directory AS (
  SELECT *
  FROM {{ ref('employee_directory_analysis') }}
),

pto_source AS (
  SELECT *
  FROM {{ ref('gitlab_pto') }}
),

date_details AS (
  SELECT *
  FROM {{ ref('date_details') }}
),

start_date AS (
  SELECT
    employee_id AS hire_id,
    date_actual AS hire_date,
    ROW_NUMBER() OVER (
      PARTITION BY employee_id ORDER BY date_actual ASC
    )           AS hire_rank
  FROM directory
  WHERE is_hire_date = 'TRUE'
    AND date_actual <= CURRENT_DATE
),

end_date AS (
  SELECT
    employee_id AS term_id,
    date_actual AS term_date,
    ROW_NUMBER() OVER (
      PARTITION BY employee_id ORDER BY date_actual ASC
    )           AS term_rank
  FROM directory
  WHERE is_termination_date = 'true'
    AND date_actual <= CURRENT_DATE
),

start_to_end AS (
  SELECT
    start_date.hire_id,
    start_date.hire_rank,
    start_date.hire_date,
    end_date.term_date,
    end_date.term_rank,
    COALESCE(end_date.term_date, CURRENT_DATE) AS last_date
  FROM start_date
  LEFT JOIN end_date ON start_date.hire_id = end_date.term_id
    AND start_date.hire_rank = end_date.term_rank
),

pto AS (
  SELECT
    *,
    DATEDIFF(DAY, start_date, end_date) + 1                AS pto_days_requested,
    DAYOFWEEK(pto_date)                                    AS pto_day_of_week,
    NOT COALESCE(total_hours < employee_day_length, FALSE) AS is_full_day,
    ROW_NUMBER() OVER (
      PARTITION BY
        hr_employee_id,
        pto_date
      ORDER BY
        end_date DESC,
        pto_uuid DESC
    )                                                      AS pto_rank,
    'Y'                                                    AS is_pto_date
  FROM pto_source
  WHERE pto_status = 'AP'
    AND pto_date <= CURRENT_DATE
    AND pto_day_of_week BETWEEN 1
    AND 5
    AND pto_days_requested <= 25
    AND COALESCE(pto_group_type, '') != 'EXL'
    AND NOT COALESCE(pto_type_name, '') IN ('CEO Shadow Program', 'Conference', 'Customer Visit')
  QUALIFY pto_rank = 1
),

dates AS (
  SELECT *
  FROM date_details
),

final AS (
  SELECT
    start_to_end.hire_id                                                                            AS employee_id,
    start_to_end.hire_date,
    start_to_end.term_date,
    start_to_end.last_date,
    dates.date_actual,
    GREATEST(start_to_end.hire_date, DATEADD('month', -11, DATE_TRUNC('month', dates.date_actual))) AS r12_start_date,
    dates.fiscal_month_name_fy,
    dates.fiscal_quarter_name_fy,
    dates.fiscal_year,
    pto.pto_group_type,
    IFF(
      pto.pto_type_name = 'Out Sick'
      AND DATEDIFF('day', pto.start_date, pto.end_date) > 4, 'Out Sick-Extended', pto.pto_type_name
    )                                                                                               AS pto_type_name,
    pto.is_pto,
    COALESCE(pto.is_pto_date, 'N')                                                                  AS is_pto_date,
    IFF(pto.is_full_day = 'TRUE', 'Y', 'N')                                                         AS is_pto_full_day,
    IFF(pto.is_holiday = 'TRUE', 'Y', 'N')                                                          AS is_pto_holiday,
    IFF(pto.is_pto_date = 'Y', 1, 0)                                                                AS pto_count,
    SUM(pto_count) OVER (
      PARTITION BY
        start_to_end.hire_id,
        start_to_end.hire_date ORDER BY start_to_end.hire_id ASC,
      dates.date_actual ASC
    )                                                                                               AS running_pto_sum,
    SUM(pto_count) OVER (
      PARTITION BY
        start_to_end.hire_id,
        start_to_end.hire_date,
        dates.fiscal_month_name_fy ORDER BY start_to_end.hire_id ASC,
      dates.date_actual ASC
    )                                                                                               AS running_pto_sum_mtd,
    SUM(pto_count) OVER (
      PARTITION BY
        start_to_end.hire_id,
        start_to_end.hire_date,
        dates.fiscal_quarter_name_fy ORDER BY start_to_end.hire_id ASC,
      dates.date_actual ASC
    )                                                                                               AS running_pto_sum_qtd,
    SUM(pto_count) OVER (
      PARTITION BY
        start_to_end.hire_id,
        start_to_end.hire_date,
        dates.fiscal_year ORDER BY start_to_end.hire_id ASC,
      dates.date_actual ASC
    )                                                                                               AS running_pto_sum_ytd,
    COUNT(pto2.is_pto_date)                                                                         AS running_pto_sum_r12
  FROM start_to_end
  LEFT JOIN dates
    ON dates.date_actual BETWEEN start_to_end.hire_date
      AND start_to_end.last_date
  LEFT JOIN pto
    ON dates.date_actual = pto.pto_date
      AND start_to_end.hire_id = pto.hr_employee_id
  LEFT JOIN
    pto
      AS pto2
    ON start_to_end.hire_id = pto2.hr_employee_id
      AND dates.date_actual >= pto2.pto_date
      AND r12_start_date <= pto2.pto_date
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
  ORDER BY 1 ASC, 5 DESC
)

SELECT
  employee_id,
  hire_date,
  term_date,
  last_date,
  date_actual,
  is_pto_date,
  is_pto_full_day,
  is_pto_holiday,
  pto_group_type,
  pto_type_name,
  running_pto_sum,
  running_pto_sum_mtd,
  running_pto_sum_qtd,
  running_pto_sum_ytd,
  running_pto_sum_r12
FROM final
