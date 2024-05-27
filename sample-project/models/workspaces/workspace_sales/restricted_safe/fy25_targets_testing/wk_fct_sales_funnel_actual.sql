{{ simple_cte([
    ('fct_crm_opportunity', 'wk_fct_crm_opportunity'),
    ('fct_crm_person', 'fct_crm_person'),
    ('prep_date', 'prep_date'),
    ('prep_order_type', 'prep_order_type')
]) }}

, first_order_key AS (

  SELECT dim_order_type_id
  FROM prep_order_type
  WHERE order_type_name = '1. New - First Order'

), net_arr AS (

  SELECT
    close_date_id                               AS actual_date_id,
    close_date::DATE                            AS actual_date,
    'Net ARR'                                   AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}               AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    crm_current_account_set_business_unit         AS business_unit_name,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    crm_current_account_set_sales_segment         AS sales_segment_name,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    crm_current_account_set_geo                   AS geo_name,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    crm_current_account_set_region                AS region_name,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    crm_current_account_set_area                  AS area_name,
    crm_current_account_set_role_name             AS role_name,
    crm_current_account_set_role_level_1          AS role_level_1,
    crm_current_account_set_role_level_2          AS role_level_2,
    crm_current_account_set_role_level_3          AS role_level_3,
    crm_current_account_set_role_level_4          AS role_level_4,
    crm_current_account_set_role_level_5          AS role_level_5,

    NULL                                        AS email_hash,
    new_logo_count,
    net_arr

  FROM fct_crm_opportunity
  WHERE fpa_master_bookings_flag = TRUE

), closed_deals AS (

  SELECT
    close_date_id                               AS actual_date_id,
    close_date::DATE                            AS actual_date,
    'Deals'                                     AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}               AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    crm_current_account_set_business_unit         AS business_unit_name,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    crm_current_account_set_sales_segment         AS sales_segment_name,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    crm_current_account_set_geo                   AS geo_name,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    crm_current_account_set_region                AS region_name,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    crm_current_account_set_area                  AS area_name,
    crm_current_account_set_role_name             AS role_name,
    crm_current_account_set_role_level_1          AS role_level_1,
    crm_current_account_set_role_level_2          AS role_level_2,
    crm_current_account_set_role_level_3          AS role_level_3,
    crm_current_account_set_role_level_4          AS role_level_4,
    crm_current_account_set_role_level_5          AS role_level_5,

    NULL AS email_hash,
    new_logo_count,
    net_arr

  FROM fct_crm_opportunity
  WHERE fpa_master_bookings_flag = TRUE

), new_logos AS (

  SELECT
    close_date_id                               AS actual_date_id,
    close_date::DATE                            AS actual_date,
    'New Logos'                                 AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}               AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    crm_current_account_set_business_unit         AS business_unit_name,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    crm_current_account_set_sales_segment         AS sales_segment_name,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    crm_current_account_set_geo                   AS geo_name,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    crm_current_account_set_region                AS region_name,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    crm_current_account_set_area                  AS area_name,
    crm_current_account_set_role_name             AS role_name,
    crm_current_account_set_role_level_1          AS role_level_1,
    crm_current_account_set_role_level_2          AS role_level_2,
    crm_current_account_set_role_level_3          AS role_level_3,
    crm_current_account_set_role_level_4          AS role_level_4,
    crm_current_account_set_role_level_5          AS role_level_5,

    NULL AS email_hash,
    new_logo_count,
    net_arr

  FROM fct_crm_opportunity
  WHERE is_new_logo_first_order = TRUE

), saos AS (

  SELECT
    sales_accepted_date_id                      AS actual_date_id,
    sales_accepted_date::DATE                   AS actual_date,
    'Stage 1 Opportunities'                     AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}               AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    crm_current_account_set_business_unit         AS business_unit_name,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    crm_current_account_set_sales_segment         AS sales_segment_name,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    crm_current_account_set_geo                   AS geo_name,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    crm_current_account_set_region                AS region_name,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    crm_current_account_set_area                  AS area_name,
    crm_current_account_set_role_name             AS role_name,
    crm_current_account_set_role_level_1          AS role_level_1,
    crm_current_account_set_role_level_2          AS role_level_2,
    crm_current_account_set_role_level_3          AS role_level_3,
    crm_current_account_set_role_level_4          AS role_level_4,
    crm_current_account_set_role_level_5          AS role_level_5,

    NULL AS email_hash,
    new_logo_count,
    net_arr
  FROM fct_crm_opportunity
  WHERE is_sao = TRUE

), pipeline_created AS (

  SELECT
    arr_created_date_id                        AS actual_date_id,
    arr_created_date::DATE                     AS actual_date,
    'Net ARR Pipeline Created'                 AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}              AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    crm_current_account_set_business_unit         AS business_unit_name,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    crm_current_account_set_sales_segment         AS sales_segment_name,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    crm_current_account_set_geo                   AS geo_name,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    crm_current_account_set_region                AS region_name,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    crm_current_account_set_area                  AS area_name,
    crm_current_account_set_role_name             AS role_name,
    crm_current_account_set_role_level_1          AS role_level_1,
    crm_current_account_set_role_level_2          AS role_level_2,
    crm_current_account_set_role_level_3          AS role_level_3,
    crm_current_account_set_role_level_4          AS role_level_4,
    crm_current_account_set_role_level_5          AS role_level_5,

    NULL                                      AS email_hash,
    new_logo_count,
    net_arr
  FROM fct_crm_opportunity
  WHERE is_net_arr_pipeline_created = TRUE
    AND arr_created_date IS NOT NULL

), mqls AS (

  SELECT
    mql_date_first_pt_id                      AS actual_date_id,
    prep_date.date_actual                     AS actual_date,
    'MQL'                                     AS sales_funnel_kpi_name,

    {{ get_keyed_nulls('NULL') }}             AS dim_crm_opportunity_id,
    dim_crm_account_id,
    dim_crm_person_id,

    -- we need to add the order_type key for first order because fct_sales_funnel_target has first order for all MQL targets
    first_order_key.dim_order_type_id,
    {{ get_keyed_nulls('NULL') }}             AS dim_sales_qualified_source_id,

    dim_account_demographics_hierarchy_sk,
    {{ get_keyed_nulls('NULL') }}             AS dim_crm_user_business_unit_id,
    NULL                                      AS business_unit_name,
    dim_account_demographics_sales_segment_id,
    NULL                                      AS sales_segment_name,
    dim_account_demographics_geo_id,
    NULL                                      AS geo_name,
    dim_account_demographics_region_id,
    NULL                                      AS region_name,
    dim_account_demographics_area_id,
    NULL                                      AS area_name,
    NULL                                      AS role_name,
    NULL                                      AS role_level_1,
    NULL                                      AS role_level_2,
    NULL                                      AS role_level_3,
    NULL                                      AS role_level_4,
    NULL                                      AS role_level_5,

    email_hash,
    NULL                                      AS new_logo_count,
    NULL                                      AS net_arr
  FROM fct_crm_person
  LEFT JOIN prep_date
    ON fct_crm_person.mql_date_first_pt_id = prep_date.date_id
  LEFT JOIN first_order_key
  WHERE mql_date_first_pt_id IS NOT NULL

), trials AS (

  SELECT
    created_date_pt_id                        AS actual_date_id,
    prep_date.date_actual                     AS actual_date,
    'Trials'                                  AS sales_funnel_kpi_name,

    {{ get_keyed_nulls('NULL') }}             AS dim_crm_opportunity_id,
    dim_crm_account_id,
    dim_crm_person_id,

    -- we need to add the order_type key for first order because fct_sales_funnel_target has first order for all MQL targets
    first_order_key.dim_order_type_id,
    {{ get_keyed_nulls('NULL') }}             AS dim_sales_qualified_source_id,

    dim_account_demographics_hierarchy_sk,
    {{ get_keyed_nulls('NULL') }}             AS dim_crm_user_business_unit_id,
    NULL                                      AS business_unit_name,
    dim_account_demographics_sales_segment_id,
    NULL                                      AS sales_segment_name,
    dim_account_demographics_geo_id,
    NULL                                      AS geo_name,
    dim_account_demographics_region_id,
    NULL                                      AS region_name,
    dim_account_demographics_area_id,
    NULL                                      AS area_name,
    NULL                                      AS role_name,
    NULL                                      AS role_level_1,
    NULL                                      AS role_level_2,
    NULL                                      AS role_level_3,
    NULL                                      AS role_level_4,
    NULL                                      AS role_level_5,

    email_hash,
    NULL                                      AS new_logo_count,
    NULL                                      AS net_arr
  FROM fct_crm_person
  LEFT JOIN prep_date
    ON fct_crm_person.created_date_pt_id = prep_date.date_id
  LEFT JOIN first_order_key
  WHERE is_lead_source_trial = TRUE

), metrics AS (

  SELECT *
  FROM net_arr

  UNION ALL

  SELECT *
  FROM closed_deals

  UNION ALL

  SELECT *
  FROM new_logos

  UNION ALL

  SELECT *
  FROM saos

  UNION ALL

  SELECT *
  FROM pipeline_created

  UNION ALL

  SELECT *
  FROM mqls

  UNION ALL

  SELECT *
  FROM trials

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['metrics.actual_date_id', 'metrics.sales_funnel_kpi_name', 'metrics.dim_crm_opportunity_id', 'metrics.dim_crm_person_id']) }} AS sales_funnel_actual_sk,
  metrics.*
FROM metrics
