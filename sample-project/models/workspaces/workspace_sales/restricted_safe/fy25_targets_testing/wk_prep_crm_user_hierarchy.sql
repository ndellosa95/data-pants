{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
    "post-hook": "{{ missing_member_column(primary_key = 'dim_crm_user_hierarchy_sk') }}"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_crm_user_daily_snapshot', 'wk_prep_crm_user_daily_snapshot'),
    ('prep_crm_user', 'wk_prep_crm_user'),
    ('prep_crm_account_daily_snapshot', 'prep_crm_account_daily_snapshot'),
    ('prep_crm_account', 'prep_crm_account'),
    ('prep_crm_opportunity', 'wk_prep_crm_opportunity_fy25'),
    ('prep_sales_funnel_target', 'wk_prep_sales_funnel_target'),
    ('prep_crm_person', 'prep_crm_person')
]) }}

, fiscal_months AS (

    SELECT DISTINCT
      fiscal_month_name_fy,
      fiscal_year,
      first_day_of_month
    FROM dim_date

), current_fiscal_year AS (

    SELECT fiscal_year
    FROM dim_date
    WHERE date_actual = CURRENT_DATE - 1

), prep_crm_user_roles AS (

    SELECT DISTINCT
      user_role_name,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      crm_user_business_unit,
      user_role_level_1,
      user_role_level_2,
      user_role_level_3,
      user_role_level_4,
      user_role_level_5
    FROM prep_crm_user

  
), account_demographics_hierarchy AS (

    SELECT DISTINCT
      created_date_fiscal_year                  AS fiscal_year,
      UPPER(account_demographics_sales_segment) AS account_demographics_sales_segment,
      UPPER(account_demographics_geo)           AS account_demographics_geo,
      UPPER(account_demographics_region)        AS account_demographics_region,
      UPPER(account_demographics_area)          AS account_demographics_area,
      NULL                                      AS user_business_unit,
      dim_account_demographics_hierarchy_sk,
      NULL                AS user_role_name,
      NULL                AS user_role_level_1,
      NULL                AS user_role_level_2,
      NULL                AS user_role_level_3,
      NULL                AS user_role_level_4,
      NULL                AS user_role_level_5
    FROM prep_crm_person 

), user_geo_hierarchy_source AS (

SELECT 
      DISTINCT
      dim_date.fiscal_year,
      prep_crm_user_daily_snapshot.crm_user_sales_segment              AS user_segment,
      prep_crm_user_daily_snapshot.crm_user_geo                        AS user_geo,
      prep_crm_user_daily_snapshot.crm_user_region                     AS user_region,
      prep_crm_user_daily_snapshot.crm_user_area                       AS user_area,
      prep_crm_user_daily_snapshot.crm_user_business_unit              AS user_business_unit,
      prep_crm_user_daily_snapshot.dim_crm_user_hierarchy_sk           AS dim_crm_user_hierarchy_sk,
      NULL                                                             AS user_role_name,
      NULL                                                             AS user_role_level_1,
      NULL                                                             AS user_role_level_2,
      NULL                                                             AS user_role_level_3,
      NULL                                                             AS user_role_level_4,
      NULL                                                             AS user_role_level_5
    FROM prep_crm_user_daily_snapshot
    INNER JOIN dim_date 
      ON prep_crm_user_daily_snapshot.snapshot_id = dim_date.date_id
    WHERE prep_crm_user_daily_snapshot.crm_user_sales_segment IS NOT NULL
        AND prep_crm_user_daily_snapshot.crm_user_geo IS NOT NULL
        AND prep_crm_user_daily_snapshot.crm_user_region IS NOT NULL
        AND prep_crm_user_daily_snapshot.crm_user_area IS NOT NULL
        AND IFF(dim_date.fiscal_year > 2023, prep_crm_user_daily_snapshot.crm_user_business_unit IS NOT NULL, 1=1) -- with the change in structure, business unit must be present after FY23
        AND IFF(dim_date.fiscal_year < dim_date.current_fiscal_year,dim_date.date_actual = dim_date.last_day_of_fiscal_year, dim_date.date_actual = dim_date.current_date_actual) -- take only the last valid hierarchy of the fiscal year for previous fiscal years
        AND dim_date.fiscal_year < 2025 -- stop geo hierarchy after 2024
        AND prep_crm_user_daily_snapshot.is_active = TRUE

), user_role_hierarchy_snapshot_source AS (
    SELECT DISTINCT
      dim_date.fiscal_year,
      prep_crm_user_daily_snapshot.crm_user_sales_segment               AS user_segment,
      prep_crm_user_daily_snapshot.crm_user_geo                         AS user_geo,
      prep_crm_user_daily_snapshot.crm_user_region                      AS user_region,
      prep_crm_user_daily_snapshot.crm_user_area                        AS user_area,
      prep_crm_user_daily_snapshot.crm_user_business_unit               AS user_business_unit,
      prep_crm_user_daily_snapshot.dim_crm_user_hierarchy_sk,
      prep_crm_user_daily_snapshot.user_role_name, 
      prep_crm_user_daily_snapshot.user_role_level_1, 
      prep_crm_user_daily_snapshot.user_role_level_2, 
      prep_crm_user_daily_snapshot.user_role_level_3, 
      prep_crm_user_daily_snapshot.user_role_level_4, 
      prep_crm_user_daily_snapshot.user_role_level_5
    FROM prep_crm_user_daily_snapshot
    INNER JOIN dim_date 
      ON prep_crm_user_daily_snapshot.snapshot_id = dim_date.date_id
    WHERE dim_date.fiscal_year >= 2025 
        AND prep_crm_user_daily_snapshot.is_active = TRUE     
    QUALIFY ROW_NUMBER() OVER (PARTITION BY prep_crm_user_daily_snapshot.user_role_name, dim_date.fiscal_year ORDER BY snapshot_id DESC) = 1

), user_role_hierarchy_live_source AS (
    SELECT DISTINCT
      current_fiscal_year.fiscal_year,
      prep_crm_user.crm_user_sales_segment                              AS user_segment,
      prep_crm_user.crm_user_geo                                        AS user_geo,
      prep_crm_user.crm_user_region                                     AS user_region,
      prep_crm_user.crm_user_area                                       AS user_area,
      prep_crm_user.crm_user_business_unit                              AS user_business_unit,
      prep_crm_user.dim_crm_user_hierarchy_sk,
      prep_crm_user.user_role_name, 
      prep_crm_user.user_role_level_1, 
      prep_crm_user.user_role_level_2, 
      prep_crm_user.user_role_level_3, 
      prep_crm_user.user_role_level_4, 
      prep_crm_user.user_role_level_5
    FROM prep_crm_user
    LEFT JOIN current_fiscal_year
    WHERE user_role_level_1 IS NOT NULL
    AND is_active = TRUE

), account_hierarchy_snapshot_source AS (

    SELECT 
      DISTINCT 
      dim_date.fiscal_year,
      prep_crm_account_daily_snapshot.parent_crm_account_sales_segment,
      prep_crm_account_daily_snapshot.parent_crm_account_geo,
      prep_crm_account_daily_snapshot.parent_crm_account_region,
      prep_crm_account_daily_snapshot.parent_crm_account_area,
      prep_crm_account_daily_snapshot.parent_crm_account_business_unit,
      prep_crm_account_daily_snapshot.dim_crm_parent_account_hierarchy_sk,
      NULL                AS user_role_name,
      NULL                AS user_role_level_1,
      NULL                AS user_role_level_2,
      NULL                AS user_role_level_3,
      NULL                AS user_role_level_4,
      NULL                AS user_role_level_5
    FROM prep_crm_account_daily_snapshot
    INNER JOIN dim_date 
      ON prep_crm_account_daily_snapshot.snapshot_id = dim_date.date_id
    WHERE prep_crm_account_daily_snapshot.parent_crm_account_sales_segment IS NOT NULL
      AND prep_crm_account_daily_snapshot.parent_crm_account_geo IS NOT NULL
      AND prep_crm_account_daily_snapshot.parent_crm_account_region IS NOT NULL
      AND prep_crm_account_daily_snapshot.parent_crm_account_area IS NOT NULL
      AND IFF(dim_date.fiscal_year > 2023, prep_crm_account_daily_snapshot.parent_crm_account_business_unit IS NOT NULL, TRUE) -- with the change in structure, business unit must be present after FY23
      AND IFF(dim_date.fiscal_year < dim_date.current_fiscal_year, dim_date.date_actual = dim_date.last_day_of_fiscal_year, dim_date.date_actual = dim_date.current_date_actual) -- take only the last valid hierarchy of the fiscal year for previous fiscal years
      AND dim_date.fiscal_year < 2025

), user_geo_hierarchy_sheetload AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the target setting model, we will union in the distinct hierarchy values from the file.
*/

    SELECT DISTINCT 
      prep_sales_funnel_target.fiscal_year,
      prep_sales_funnel_target.user_segment,
      prep_sales_funnel_target.user_geo,
      prep_sales_funnel_target.user_region,
      prep_sales_funnel_target.user_area,
      prep_sales_funnel_target.user_business_unit,
      prep_sales_funnel_target.dim_crm_user_hierarchy_sk,
      NULL                AS user_role_name,
      NULL                AS user_role_level_1,
      NULL                AS user_role_level_2,
      NULL                AS user_role_level_3,
      NULL                AS user_role_level_4,
      NULL                AS user_role_level_5
    FROM prep_sales_funnel_target
    WHERE prep_sales_funnel_target.user_area != 'N/A'
      AND prep_sales_funnel_target.user_segment IS NOT NULL
      AND prep_sales_funnel_target.user_geo IS NOT NULL
      AND prep_sales_funnel_target.user_region IS NOT NULL
      AND prep_sales_funnel_target.user_area IS NOT NULL
      AND prep_sales_funnel_target.role_level_1 IS NULL

), user_role_hierarchy_sheetload AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the target setting model, we will union in the distinct hierarchy values from the file.
*/

    SELECT DISTINCT  
      prep_sales_funnel_target.fiscal_year,
      prep_crm_user_roles.crm_user_sales_segment                      AS user_segment,
      prep_crm_user_roles.crm_user_geo                                AS user_geo,
      prep_crm_user_roles.crm_user_region                             AS user_region,
      prep_crm_user_roles.crm_user_area                               AS user_area,
      prep_crm_user_roles.crm_user_business_unit                      AS user_business_unit,
      prep_sales_funnel_target.dim_crm_user_hierarchy_sk,
      prep_sales_funnel_target.user_role_name,
      prep_sales_funnel_target.role_level_1,
      prep_sales_funnel_target.role_level_2,
      prep_sales_funnel_target.role_level_3,
      prep_sales_funnel_target.role_level_4,
      prep_sales_funnel_target.role_level_5
    FROM prep_sales_funnel_target
    LEFT JOIN prep_crm_user_roles
      ON prep_sales_funnel_target.user_role_name = prep_crm_user_roles.user_role_name
    WHERE prep_sales_funnel_target.role_level_1 IS NOT NULL

), user_geo_hierarchy_stamped_opportunity AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the stamped opportunities, we will union in the distinct hierarchy values from the stamped opportunities.
  The hierarchy switched from geo to role after 2024 so we stop taking geo values after that fiscal_year.
*/

    SELECT DISTINCT
      prep_crm_opportunity.close_fiscal_year                         AS fiscal_year,
      prep_crm_opportunity.user_segment_stamped                      AS user_segment,
      prep_crm_opportunity.user_geo_stamped                          AS user_geo,
      prep_crm_opportunity.user_region_stamped                       AS user_region,
      prep_crm_opportunity.user_area_stamped                         AS user_area,
      prep_crm_opportunity.user_business_unit_stamped                AS user_business_unit,
      prep_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk    AS dim_crm_user_hierarchy_sk,
      NULL                                                           AS user_role_name,
      NULL                                                           AS user_role_level_1,
      NULL                                                           AS user_role_level_2,
      NULL                                                           AS user_role_level_3,
      NULL                                                           AS user_role_level_4,
      NULL                                                           AS user_role_level_5
    FROM prep_crm_opportunity
    WHERE is_live = 1
    AND prep_crm_opportunity.close_fiscal_year < 2025

), user_role_hierarchy_stamped_opportunity AS (
/*
  To get a complete picture of the hierarchy and to ensure fidelity with the stamped opportunities, we will union in the distinct hierarchy values from the stamped opportunities.
  The hierarchy switched from geo to role after 2024 so only take role values after that fiscal_year.
*/

    SELECT DISTINCT
      prep_crm_opportunity.close_fiscal_year                         AS fiscal_year,
      prep_crm_user_roles.crm_user_sales_segment                     AS user_segment,
      prep_crm_user_roles.crm_user_geo                               AS user_geo,
      prep_crm_user_roles.crm_user_region                            AS user_region,
      prep_crm_user_roles.crm_user_area                              AS user_area,
      prep_crm_user_roles.crm_user_business_unit                     AS user_business_unit,
      prep_crm_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk    AS dim_crm_user_hierarchy_sk,
      prep_crm_opportunity.opportunity_owner_role                    AS user_role_name,
      prep_crm_user_roles.user_role_level_1                          AS user_role_level_1,
      prep_crm_user_roles.user_role_level_2                          AS user_role_level_2,
      prep_crm_user_roles.user_role_level_3                          AS user_role_level_3,
      prep_crm_user_roles.user_role_level_4                          AS user_role_level_4,
      prep_crm_user_roles.user_role_level_5                          AS user_role_level_5
    FROM prep_crm_opportunity
    INNER JOIN prep_crm_user_roles
      ON prep_crm_opportunity.opportunity_owner_role = prep_crm_user_roles.user_role_name 
    WHERE is_live = 1
      AND prep_crm_user_roles.user_role_level_1 IS NOT NULL
      AND prep_crm_opportunity.close_fiscal_year >= 2025
  
), unioned AS (
/*
  Union all four hierarchy sources to combine all possible hierarchies generated used in the past, as well as those not currently used in the system, but used in target setting.
*/

    SELECT *
    FROM user_geo_hierarchy_source

    UNION

    SELECT *
    FROM user_role_hierarchy_snapshot_source
    WHERE user_role_level_1 IS NOT NULL       

    UNION

    SELECT *
    FROM user_role_hierarchy_live_source
  
    UNION
 
    SELECT *
    FROM user_geo_hierarchy_sheetload

    UNION

    SELECT *
    FROM user_role_hierarchy_sheetload

    UNION

    SELECT *
    FROM user_geo_hierarchy_stamped_opportunity

    UNION

    SELECT *
    FROM user_role_hierarchy_stamped_opportunity

    UNION

    SELECT *
    FROM account_hierarchy_snapshot_source

    UNION

    SELECT *
    FROM account_demographics_hierarchy

), pre_fy24_hierarchy AS (

/*
  Before FY24, the hierarchy only uncluded segment, geo, region, and area.
*/

    SELECT DISTINCT
      fiscal_year,
      UPPER(user_segment)       AS user_segment,
      UPPER(user_geo)           AS user_geo,
      UPPER(user_region)        AS user_region,
      UPPER(user_area)          AS user_area,
      NULL                      AS user_business_unit,
      dim_crm_user_hierarchy_sk,
      NULL                      AS user_role_name,
      NULL                      AS user_role_level_1,
      NULL                      AS user_role_level_2,
      NULL                      AS user_role_level_3,
      NULL                      AS user_role_level_4,
      NULL                      AS user_role_level_5
    FROM unioned 
    WHERE fiscal_year < 2024

), fy24_hierarchy AS (

/*
  In FY24, business unit was added to the hierarchy.
*/


    SELECT DISTINCT
      fiscal_year,
      UPPER(user_segment)       AS user_segment,
      UPPER(user_geo)           AS user_geo,
      UPPER(user_region)        AS user_region,
      UPPER(user_area)          AS user_area,
      UPPER(user_business_unit) AS user_business_unit,
      dim_crm_user_hierarchy_sk,
      NULL                      AS user_role_name,
      NULL                      AS user_role_level_1,
      NULL                      AS user_role_level_2,
      NULL                      AS user_role_level_3,
      NULL                      AS user_role_level_4,
      NULL                      AS user_role_level_5
    FROM unioned 
    WHERE fiscal_year = 2024

), fy25_and_beyond_hierarchy AS (

/*
  In FY25, we switched to a role based hierarchy.
*/


    SELECT DISTINCT
      fiscal_year,
      UPPER(user_segment)           AS user_segment,
      UPPER(user_geo)               AS user_geo,
      UPPER(user_region)            AS user_region,
      UPPER(user_area)              AS user_area,
      UPPER(user_business_unit)     AS user_business_unit,
      dim_crm_user_hierarchy_sk,
      UPPER(user_role_name)         AS user_role_name,
      UPPER(user_role_level_1)      AS user_role_level_1,
      UPPER(user_role_level_2)      AS user_role_level_2,
      MIN(UPPER(user_role_level_3)) AS user_role_level_3, -- workaround linked to https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/5181
      MIN(UPPER(user_role_level_4)) AS user_role_level_4, -- hopefully the MIN function can be removed before merging to production.
      MIN(UPPER(user_role_level_5)) AS user_role_level_5
    FROM unioned 
    WHERE fiscal_year >= 2025
    {{ dbt_utils.group_by(n=10)}}

), final_unioned AS (


    SELECT *
    FROM pre_fy24_hierarchy

    UNION ALL

    SELECT *
    FROM fy24_hierarchy

    UNION ALL

    SELECT *
    FROM fy25_and_beyond_hierarchy

), final AS (

    SELECT DISTINCT 
      {{ dbt_utils.generate_surrogate_key(['final_unioned.dim_crm_user_hierarchy_sk']) }}                                       AS dim_crm_user_hierarchy_id,
      final_unioned.dim_crm_user_hierarchy_sk,
      final_unioned.fiscal_year,
      final_unioned.user_business_unit                                                                                          AS crm_user_business_unit,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_business_unit']) }}                                              AS dim_crm_user_business_unit_id,
      final_unioned.user_segment                                                                                                AS crm_user_sales_segment,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_segment']) }}                                                    AS dim_crm_user_sales_segment_id,
      final_unioned.user_geo                                                                                                    AS crm_user_geo,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_geo']) }}                                                        AS dim_crm_user_geo_id,
      final_unioned.user_region                                                                                                 AS crm_user_region,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_region']) }}                                                     AS dim_crm_user_region_id,
      final_unioned.user_area                                                                                                   AS crm_user_area,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_area']) }}                                                       AS dim_crm_user_area_id,
      final_unioned.user_role_name                                                                                              AS crm_user_role_name,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_role_name']) }}                                                  AS dim_crm_user_role_name_id,
      final_unioned.user_role_level_1                                                                                           AS crm_user_role_level_1,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_role_level_1']) }}                                               AS dim_crm_user_role_level_1_id,
      final_unioned.user_role_level_2                                                                                           AS crm_user_role_level_2,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_role_level_2']) }}                                               AS dim_crm_user_role_level_2_id,
      final_unioned.user_role_level_3                                                                                           AS crm_user_role_level_3,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_role_level_3']) }}                                               AS dim_crm_user_role_level_3_id,
      final_unioned.user_role_level_4                                                                                           AS crm_user_role_level_4,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_role_level_4']) }}                                               AS dim_crm_user_role_level_4_id,
      final_unioned.user_role_level_5                                                                                           AS crm_user_role_level_5,
      {{ dbt_utils.generate_surrogate_key(['final_unioned.user_role_level_5']) }}                                               AS dim_crm_user_role_level_5_id,
      CASE
          WHEN final_unioned.user_segment IN ('Large', 'PubSec') THEN 'Large'
          ELSE final_unioned.user_segment
      END                                                                                                                       AS crm_user_sales_segment_grouped,
      {{ sales_segment_region_grouped('final_unioned.user_segment', 'final_unioned.user_geo', 'final_unioned.user_region') }}   AS crm_user_sales_segment_region_grouped,
      IFF(final_unioned.fiscal_year = current_fiscal_year.fiscal_year, 1, 0)                                                    AS is_current_crm_user_hierarchy
    FROM final_unioned
    LEFT JOIN current_fiscal_year
      ON final_unioned.fiscal_year = current_fiscal_year.fiscal_year
    WHERE dim_crm_user_hierarchy_sk IS NOT NULL

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@chrissharp",
    created_date="2021-01-05",
    updated_date="2024-04-23"
) }}
