WITH cost_center AS (

  /* We have a Islands problem in the cost_center calculation
  We need to find groups based on when the cost center changes and not just the value of the cost_center
  Example: We have team members that belong to one cost_center, get moved to a different one but then return to the
  initial cost_center afterwards
  */

  SELECT 
    employee_id,
    cost_center,
    DATE(valid_from)                                                                                           AS valid_from,
    DATE(valid_to)                                                                                             AS valid_to,
    LAG(cost_center, 1, NULL) OVER (PARTITION BY employee_id ORDER BY valid_from)                              AS lag_cost_center,
    CONDITIONAL_TRUE_EVENT(cost_center != lag_cost_center) OVER (PARTITION BY employee_id ORDER BY valid_from) AS cost_center_group,
    LEAD(valid_from,1) OVER (PARTITION BY employee_id ORDER BY valid_from)                                     AS next_entry
  FROM {{ ref('workday_employee_mapping_source') }} 
  
),

cost_center_group AS (

  SELECT 
    employee_id, 
    cost_center, 
    cost_center_group, 
    MIN(valid_from)                                                                                            AS valid_from,
    MAX(valid_to)                                                                                              AS valid_to 
  FROM cost_center
  {{ dbt_utils.group_by(n=3)}}
  
),

team_member AS (

  SELECT 
    dim_team_member_sk,
    employee_id,
    nationality,
    ethnicity,
    first_name,
    last_name,
    first_name || ' ' || last_name                                                                  AS full_name,
    gender,
    work_email,
    date_of_birth,
    DATEDIFF(year, date_of_birth, valid_from) + CASE WHEN (DATEADD(year,DATEDIFF(year, date_of_birth, valid_from) , date_of_birth) > valid_from) THEN - 1 ELSE 0 END 
                                                                                                    AS age_calculated,
    CASE 
      WHEN age_calculated BETWEEN 18 AND 24  THEN '18-24'
      WHEN age_calculated BETWEEN 25 AND 29  THEN '25-29'
      WHEN age_calculated BETWEEN 30 AND 34  THEN '30-34'
      WHEN age_calculated BETWEEN 35 AND 39  THEN '35-39'
      WHEN age_calculated BETWEEN 40 AND 44  THEN '40-44'
      WHEN age_calculated BETWEEN 44 AND 49  THEN '44-49'
      WHEN age_calculated BETWEEN 50 AND 54  THEN '50-54'
      WHEN age_calculated BETWEEN 55 AND 59  THEN '55-59'
      WHEN age_calculated >= 60              THEN '60+'
      WHEN age_calculated IS NULL            THEN 'Unreported'
      WHEN age_calculated = -1               THEN 'Unreported'
      ELSE NULL 
    END                                                                                             AS age_cohort,
    gitlab_username,
    team_id,
    country,
    region,
    CASE
      WHEN region = 'Americas' AND country IN ('United States', 'Canada','Mexico','United States of America') 
        THEN 'NORAM'
      WHEN region = 'Americas' AND country NOT IN ('United States', 'Canada','Mexico','United States of America') 
        THEN 'LATAM'
      ELSE region END                                                                               AS region_modified,
    IFF(country IN ('United States','United States of America'), 
      COALESCE(gender,'Did Not Identify')  || '_' || 'United States of America', 
      COALESCE(gender,'Did Not Identify')  || '_'|| 'Non-US')                                       AS gender_region,
    IFF(country IN ('United States','United States of America'), 
      COALESCE(ethnicity,'Did Not Identify')  || '_' || 'United States of America', 
      COALESCE(ethnicity,'Did Not Identify')  || '_'|| 'Non-US')                                    AS ethnicity_region,
    CASE
      WHEN COALESCE(ethnicity, 'Did Not Identify') NOT IN ('White','Asian','Did Not Identify','Declined to Answer')
          THEN TRUE
      ELSE FALSE END                                                                                AS urg_group,
    IFF(urg_group = TRUE, 'URG', 'Non-URG')  || '_' ||
        IFF(country IN ('United States','United States of America'),
          'United States of America',
          'Non-US')                                                                                 AS urg_region,
    hire_date,
    employee_type,
    termination_date,
    is_current_team_member,
    is_rehire,
    valid_from,
    valid_to
  FROM {{ ref('dim_team_member') }}

),

team_member_position AS (

  SELECT 
    dim_team_member_sk                                                                            AS dim_team_member_sk,
    dim_team_sk                                                                                   AS dim_team_sk,
    employee_id                                                                                   AS employee_id,
    COALESCE(team_id,'Unknown Team ID')                                                           AS team_id,
    manager                                                                                       AS manager,
    COALESCE(suporg,'Unknown Supervisory Organization')                                           AS suporg,
    COALESCE(job_code,'Unknown Job Code')                                                         AS job_code,
    position                                                                                      AS position,
    COALESCE(job_family,'Unknown Job Family')                                                     AS job_family,
    job_specialty_single                                                                          AS job_specialty_single,
    job_specialty_multi                                                                           AS job_specialty_multi,
    COALESCE(management_level,'Unknown Management Level')                                         AS management_level,
    COALESCE(job_grade,'Unknown Job Grade')                                                       AS job_grade,
    department                                                                                    AS department,
    division                                                                                      AS division,
    entity                                                                                        AS entity,
    valid_from                                                                                    AS valid_from,
    valid_to                                                                                      AS valid_to
  FROM {{ ref('fct_team_member_position') }}


), 

unioned_dates AS (

  SELECT 
    employee_id, 
    NULL AS team_id,
    valid_from
  FROM cost_center_group
  
  UNION

  SELECT 
    employee_id, 
    team_id,
    valid_from
  FROM team_member
  
  UNION

  SELECT 
    employee_id, 
    team_id,
    valid_from
  FROM team_member_position

),

date_range AS (

  SELECT 
    employee_id,
    team_id,
    valid_from,
    LEAD(valid_from, 1, {{var('tomorrow')}}) OVER (PARTITION BY employee_id ORDER BY valid_from) AS valid_to,
    IFF(valid_to = {{var('tomorrow')}}, TRUE, FALSE) AS is_current
  FROM unioned_dates
  
),

final AS (

  SELECT 

    -- Surrogate keys
    team_member.dim_team_member_sk,
    {{ dbt_utils.generate_surrogate_key(['team_member.team_id']) }}                AS dim_team_sk,

    --Natural keys
    team_member.employee_id,

    --Team member info
    team_member.nationality,
    team_member.ethnicity,
    team_member.first_name,
    team_member.last_name,
    team_member.full_name,
    team_member.gender,
    team_member.work_email,
    team_member.date_of_birth,
    team_member.age_calculated                                            AS age,
    team_member.age_cohort,
    COALESCE(cost_center_group.cost_center,'Unknown Cost Center')         AS cost_center,
    team_member.gitlab_username,
    team_member.country,
    team_member.region,
    team_member.region_modified,
    team_member.gender_region,
    team_member.ethnicity_region,
    team_member.urg_group,
    team_member.urg_region,
    team_member.hire_date,
    team_member.employee_type,
    team_member.termination_date,
    team_member.is_current_team_member,
    team_member.is_rehire,
    team_member.team_id,
    team_member_position.manager                                          AS team_manager_name,
    team_member_position.department                                       AS department,
    LAST_VALUE(team_member_position.division IGNORE NULLS) OVER (PARTITION BY date_range.employee_id ORDER BY date_range.valid_from ROWS UNBOUNDED PRECEDING)           
                                                                          AS division,
    team_member_position.suporg,
    team_member_position.job_code,
    team_member_position.position,
    team_member_position.job_family,
    team_member_position.job_specialty_single,
    team_member_position.job_specialty_multi,
    team_member_position.management_level,
    team_member_position.job_grade,
    team_member_position.entity,
    date_range.valid_from,
    date_range.valid_to,
    date_range.is_current
  FROM team_member
  INNER JOIN date_range
    ON date_range.employee_id = team_member.employee_id 
      AND date_range.valid_from != date_range.valid_to
        AND NOT (team_member.valid_to <= date_range.valid_from
          OR team_member.valid_from >= date_range.valid_to)
  LEFT JOIN cost_center_group
    ON date_range.employee_id = cost_center_group.employee_id 
      AND NOT (cost_center_group.valid_to <= date_range.valid_from
          OR cost_center_group.valid_from >= date_range.valid_to)
  LEFT JOIN team_member_position
    ON date_range.employee_id = team_member_position.employee_id 
      AND NOT (team_member_position.valid_to <= date_range.valid_from
          OR team_member_position.valid_from >= date_range.valid_to)

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2023-07-06",
    updated_date="2023-09-26"
) }}
