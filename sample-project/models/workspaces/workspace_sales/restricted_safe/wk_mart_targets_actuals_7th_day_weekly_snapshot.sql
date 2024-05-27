WITH targets_actuals AS (

  SELECT *
  FROM {{ ref('wk_fct_targets_actuals_7th_day_weekly_snapshot') }}

),

dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}

),

dim_crm_user_hierarchy AS (

  SELECT *
  FROM {{ ref('wk_dim_crm_user_hierarchy') }}

),

sales_qualified_source AS (

  SELECT *
  FROM {{ ref('dim_sales_qualified_source') }}

),

order_type AS (

  SELECT *
  FROM {{ ref('dim_order_type') }}

),

final AS (

  SELECT 
    targets_actuals.*,
    DAYNAME(dim_date.current_date_actual) AS current_day_name,  --need to add this field to date_details
    dim_date.current_date_actual,
    dim_date.current_fiscal_year,
    dim_date.current_first_day_of_fiscal_year,
    dim_date.current_fiscal_quarter_name_fy,
    dim_date.current_first_day_of_month,
    dim_date.current_first_day_of_fiscal_quarter,
    dim_date.current_day_of_month,
    dim_date.current_day_of_fiscal_quarter,
    dim_date.current_day_of_fiscal_year,
    CASE WHEN current_day_name = 'Sun' THEN dim_date.current_date_actual
      ELSE DATEADD('day', -1, DATE_TRUNC('week', dim_date.current_date_actual)) END     
                                                                    AS current_first_day_of_week,
    FLOOR((DATEDIFF(day, dim_date.current_first_day_of_fiscal_quarter, dim_date.current_date_actual) / 7))                   
                                                                    AS current_week_of_fiscal_quarter_normalised,
    FLOOR((DATEDIFF(day, dim_date.current_first_day_of_fiscal_quarter, dim_date.current_date_actual) / 7)) 
                                                                    AS current_week_of_fiscal_quarter,
    dim_date.date_day                                               AS snapshot_day,
    dim_date.day_name                                               AS snapshot_day_name, 
    dim_date.day_of_week                                            AS snapshot_day_of_week,
    dim_date.first_day_of_week                                      AS snapshot_first_day_of_week,
    dim_date.week_of_year                                           AS snapshot_week_of_year,
    dim_date.day_of_month                                           AS snapshot_day_of_month,
    dim_date.day_of_quarter                                         AS snapshot_day_of_quarter,
    dim_date.day_of_year                                            AS snapshot_day_of_year,
    dim_date.fiscal_quarter                                         AS snapshot_fiscal_quarter,
    dim_date.day_of_fiscal_quarter                                  AS snapshot_day_of_fiscal_quarter,
    dim_date.day_of_fiscal_year                                     AS snapshot_day_of_fiscal_year,
    dim_date.month_name                                             AS snapshot_month_name,
    dim_date.first_day_of_month                                     AS snapshot_first_day_of_month,
    dim_date.last_day_of_month                                      AS snapshot_last_day_of_month,
    dim_date.first_day_of_year                                      AS snapshot_first_day_of_year,
    dim_date.last_day_of_year                                       AS snapshot_last_day_of_year,
    dim_date.first_day_of_quarter                                   AS snapshot_first_day_of_quarter,
    dim_date.last_day_of_quarter                                    AS snapshot_last_day_of_quarter,
    dim_date.first_day_of_fiscal_quarter                            AS snapshot_first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter                             AS snapshot_last_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year                               AS snapshot_first_day_of_fiscal_year,
    dim_date.last_day_of_fiscal_year                                AS snapshot_last_day_of_fiscal_year,
    dim_date.week_of_fiscal_year                                    AS snapshot_week_of_fiscal_year,
    dim_date.month_of_fiscal_year                                   AS snapshot_month_of_fiscal_year,
    dim_date.last_day_of_week                                       AS snapshot_last_day_of_week,
    dim_date.quarter_name                                           AS snapshot_quarter_name,
    dim_date.fiscal_quarter_name_fy                                 AS snapshot_fiscal_quarter_name_fy,
    dim_date.fiscal_quarter_number_absolute                         AS snapshot_fiscal_quarter_number_absolute,
    dim_date.fiscal_month_name                                      AS snapshot_fiscal_month_name,
    dim_date.fiscal_month_name_fy                                   AS snapshot_fiscal_month_name_fy,
    dim_date.holiday_desc                                           AS snapshot_holiday_desc,
    dim_date.is_holiday                                             AS snapshot_is_holiday,
    dim_date.last_month_of_fiscal_quarter                           AS snapshot_last_month_of_fiscal_quarter,
    dim_date.is_first_day_of_last_month_of_fiscal_quarter           AS snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    dim_date.last_month_of_fiscal_year                              AS snapshot_last_month_of_fiscal_year,
    dim_date.is_first_day_of_last_month_of_fiscal_year              AS snapshot_is_first_day_of_last_month_of_fiscal_year,
    dim_date.days_in_month_count                                    AS snapshot_days_in_month_count,
    dim_date.week_of_month_normalised                               AS snapshot_week_of_month_normalised,
    dim_date.week_of_fiscal_quarter_normalised                      AS snapshot_week_of_fiscal_quarter_normalised,
    dim_date.is_first_day_of_fiscal_quarter_week                    AS snapshot_is_first_day_of_fiscal_quarter_week,
    dim_date.days_until_last_day_of_month                           AS snapshot_days_until_last_day_of_month,
    FLOOR((DATEDIFF(day, dim_date.first_day_of_fiscal_quarter, targets_actuals.date_actual) / 7)) 
                                                                    AS snapshot_week_of_fiscal_quarter,
    dim_crm_user_hierarchy.crm_user_role_name                       AS crm_current_account_set_role_name,
    dim_crm_user_hierarchy.crm_user_role_level_1                    AS crm_current_account_set_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2                    AS crm_current_account_set_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3                    AS crm_current_account_set_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4                    AS crm_current_account_set_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5                    AS crm_current_account_set_role_level_5,
    sales_qualified_source.sales_qualified_source_name,
    sales_qualified_source.sales_qualified_source_grouped,
    order_type.order_type_name                                      AS order_type,
    order_type.order_type_grouped
  FROM targets_actuals
  LEFT JOIN dim_date 
    ON targets_actuals.date_actual = dim_date.date_actual
  LEFT JOIN dim_crm_user_hierarchy
    ON targets_actuals.dim_crm_user_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk 
  LEFT JOIN sales_qualified_source
    ON targets_actuals.dim_sales_qualified_source_id = sales_qualified_source.dim_sales_qualified_source_id 
  LEFT JOIN order_type
    ON targets_actuals.dim_order_type_id = order_type.dim_order_type_id 


)

SELECT *
FROM final