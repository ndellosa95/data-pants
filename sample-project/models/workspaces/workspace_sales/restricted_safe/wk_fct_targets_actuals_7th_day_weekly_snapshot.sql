WITH targets AS (

  SELECT *
  FROM {{ ref('wk_fct_sales_funnel_target_daily') }}

),

actuals AS (

  SELECT *
  FROM {{ ref('wk_fct_crm_opportunity_daily_snapshot') }}

),

dim_date AS (

  SELECT *
  FROM {{ ref('dim_date') }}


),

spine AS (

  {{ date_spine_7th_day() }}

),

total_targets AS (

  SELECT
    dim_date.fiscal_quarter_name_fy      AS fiscal_quarter_name,
    dim_date.first_day_of_fiscal_quarter AS fiscal_quarter_date,
    targets.dim_crm_user_hierarchy_sk,
    targets.dim_sales_qualified_source_id,
    targets.dim_order_type_id,
    targets.geo_name,
    targets.region_name,
    targets.area_name,
    targets.sales_segment_name,
    targets.business_unit_name,
    SUM(daily_allocated_target)          AS total_quarter_target
  FROM targets
  LEFT JOIN dim_date
    ON targets.target_date_id = dim_date.date_id
  WHERE kpi_name = 'Net ARR'
  {{ dbt_utils.group_by(n=10) }}
),


daily_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.snapshot_date,
    actuals.snapshot_id,
    actuals.dim_crm_current_account_set_hierarchy_sk AS dim_crm_user_hierarchy_sk,
    actuals.dim_sales_qualified_source_id,
    actuals.dim_order_type_id,
    crm_current_account_set_sales_segment_live AS crm_current_account_set_sales_segment,
    crm_current_account_set_geo_live AS crm_current_account_set_geo,
    crm_current_account_set_region_live AS crm_current_account_set_region,
    crm_current_account_set_area_live AS crm_current_account_set_area,
    crm_current_account_set_business_unit_live AS crm_current_account_set_business_unit,
    SUM(booked_net_arr_in_snapshot_quarter)          AS booked_net_arr_in_snapshot_quarter,
    SUM(open_1plus_net_arr_in_snapshot_quarter)      AS open_1plus_net_arr_in_snapshot_quarter,
    SUM(open_3plus_net_arr_in_snapshot_quarter)      AS open_3plus_net_arr_in_snapshot_quarter,
    SUM(open_4plus_net_arr_in_snapshot_quarter)      AS open_4plus_net_arr_in_snapshot_quarter

  FROM actuals
  {{ dbt_utils.group_by(n=12) }}
),

quarterly_actuals AS (

  SELECT
    actuals.snapshot_fiscal_quarter_name,
    actuals.snapshot_fiscal_quarter_date,
    actuals.dim_crm_current_account_set_hierarchy_sk AS dim_crm_user_hierarchy_sk,
    actuals.dim_sales_qualified_source_id,
    actuals.dim_order_type_id,
    crm_current_account_set_sales_segment_live AS crm_current_account_set_sales_segment,
    crm_current_account_set_geo_live AS crm_current_account_set_geo,
    crm_current_account_set_region_live AS crm_current_account_set_region,
    crm_current_account_set_area_live AS crm_current_account_set_area,
    crm_current_account_set_business_unit_live AS crm_current_account_set_business_unit,
    SUM(actuals.booked_net_arr_in_snapshot_quarter)  AS total_booked_net_arr
  FROM actuals
  WHERE snapshot_date = snapshot_last_day_of_fiscal_quarter
  {{ dbt_utils.group_by(n=10) }}

),

combined_data AS ( 

    SELECT 
        dim_crm_user_hierarchy_sk,
        dim_sales_qualified_source_id,
        dim_order_type_id,
        sales_segment_name  AS crm_current_account_set_sales_segment,
        geo_name            AS crm_current_account_set_geo,
        region_name         AS crm_current_account_set_region,
        area_name           AS crm_current_account_set_area,
        business_unit_name  AS crm_current_account_set_business_unit,
        fiscal_quarter_name,
        fiscal_quarter_date
    FROM total_targets

    UNION

    SELECT 
        dim_crm_user_hierarchy_sk,
        dim_sales_qualified_source_id,
        dim_order_type_id,
        crm_current_account_set_sales_segment,
        crm_current_account_set_geo,
        crm_current_account_set_region,
        crm_current_account_set_area,
        crm_current_account_set_business_unit,
        snapshot_fiscal_quarter_name,
        snapshot_fiscal_quarter_date
    FROM quarterly_actuals

    UNION 

    SELECT 
        dim_crm_user_hierarchy_sk,
        dim_sales_qualified_source_id,
        dim_order_type_id,
        crm_current_account_set_sales_segment,
        crm_current_account_set_geo,
        crm_current_account_set_region,
        crm_current_account_set_area,
        crm_current_account_set_business_unit,
        snapshot_fiscal_quarter_name,
        snapshot_fiscal_quarter_date
    FROM daily_actuals

),

base AS (

  /*
    Cross join all dimensions (hierarchy, qualified source, order type) and
    the dates to create a comprehensive set of all possible combinations of these dimensions and dates.
    This exhaustive combination is essential for scenarios where we need to account for all possible configurations in our analysis,
    ensuring that no combination is overlooked.

    When we eventually join this comprehensive set of combinations with the quarterly actuals,
    it ensures that even the newly introduced dimensions are accounted for.
  */

  SELECT   
    combined_data.dim_crm_user_hierarchy_sk,
    combined_data.dim_sales_qualified_source_id,
    combined_data.dim_order_type_id,     
    combined_data.crm_current_account_set_sales_segment,    
    combined_data.crm_current_account_set_geo,    
    combined_data.crm_current_account_set_region,    
    combined_data.crm_current_account_set_area,    
    combined_data.crm_current_account_set_business_unit,
    spine.date_id,
    spine.day_7 as date_actual,
    combined_data.fiscal_quarter_date,
    combined_data.fiscal_quarter_name
  FROM combined_data
  INNER JOIN spine
      ON combined_data.fiscal_quarter_name = spine.fiscal_quarter_name

),

final AS (

  SELECT
    base.date_id,
    base.date_actual,
    base.fiscal_quarter_name,
    base.fiscal_quarter_date,
    base.dim_crm_user_hierarchy_sk,
    base.dim_order_type_id,
    base.dim_sales_qualified_source_id,
    base.crm_current_account_set_sales_segment,    
    base.crm_current_account_set_geo,    
    base.crm_current_account_set_region,    
    base.crm_current_account_set_area,    
    base.crm_current_account_set_business_unit,
    SUM(total_targets.total_quarter_target)                   AS total_quarter_target,
    SUM(daily_actuals.booked_net_arr_in_snapshot_quarter)     AS coverage_booked_net_arr,
    SUM(daily_actuals.open_1plus_net_arr_in_snapshot_quarter) AS coverage_open_1plus_net_arr,
    SUM(daily_actuals.open_3plus_net_arr_in_snapshot_quarter) AS coverage_open_3plus_net_arr,
    SUM(daily_actuals.open_4plus_net_arr_in_snapshot_quarter) AS coverage_open_4plus_net_arr,
    SUM(quarterly_actuals.total_booked_net_arr)               AS total_booked_net_arr
  FROM base
  LEFT JOIN total_targets
    ON base.fiscal_quarter_name = total_targets.fiscal_quarter_name
      AND base.dim_sales_qualified_source_id = total_targets.dim_sales_qualified_source_id
      AND base.dim_crm_user_hierarchy_sk = total_targets.dim_crm_user_hierarchy_sk
      AND base.dim_order_type_id = total_targets.dim_order_type_id
      AND base.crm_current_account_set_sales_segment = total_targets.sales_segment_name
      AND base.crm_current_account_set_geo = total_targets.geo_name
      AND base.crm_current_account_set_region = total_targets.region_name
      AND base.crm_current_account_set_business_unit = total_targets.business_unit_name
      AND base.crm_current_account_set_area = total_targets.area_name
  LEFT JOIN daily_actuals
    ON base.date_id = daily_actuals.snapshot_id
      AND base.dim_sales_qualified_source_id = daily_actuals.dim_sales_qualified_source_id
      AND base.dim_crm_user_hierarchy_sk = daily_actuals.dim_crm_user_hierarchy_sk
      AND base.dim_order_type_id = daily_actuals.dim_order_type_id
      AND base.crm_current_account_set_sales_segment = daily_actuals.crm_current_account_set_sales_segment
      AND base.crm_current_account_set_geo = daily_actuals.crm_current_account_set_geo
      AND base.crm_current_account_set_region = daily_actuals.crm_current_account_set_region
      AND base.crm_current_account_set_business_unit = daily_actuals.crm_current_account_set_business_unit
      AND base.crm_current_account_set_area = daily_actuals.crm_current_account_set_area
  LEFT JOIN quarterly_actuals
    ON base.fiscal_quarter_name = quarterly_actuals.snapshot_fiscal_quarter_name
      AND base.dim_sales_qualified_source_id = quarterly_actuals.dim_sales_qualified_source_id
      AND base.dim_crm_user_hierarchy_sk = quarterly_actuals.dim_crm_user_hierarchy_sk
      AND base.dim_order_type_id = quarterly_actuals.dim_order_type_id
      AND base.crm_current_account_set_sales_segment = quarterly_actuals.crm_current_account_set_sales_segment
      AND base.crm_current_account_set_geo = quarterly_actuals.crm_current_account_set_geo
      AND base.crm_current_account_set_region = quarterly_actuals.crm_current_account_set_region
      AND base.crm_current_account_set_business_unit = quarterly_actuals.crm_current_account_set_business_unit
      AND base.crm_current_account_set_area = quarterly_actuals.crm_current_account_set_area
  {{ dbt_utils.group_by(n=12) }}

)

SELECT *
FROM final