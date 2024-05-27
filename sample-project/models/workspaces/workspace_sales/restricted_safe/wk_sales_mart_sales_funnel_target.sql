  
  {{ config(alias='mart_sales_funnel_target') }}


   WITH date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM prod.workspace_sales.date_details

 ), mart_sales_funnel_target AS (

      SELECT *
      FROM {{ref('mart_sales_funnel_target')}}
      --FROM nfiguera_prod.restricted_safe_common_mart_sales.mart_sales_funnel_target

 ), agg_demo_keys AS (
  -- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }}
    --FROM nfiguera_prod.restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

), mart_sales_funnel_target_prep AS (

    SELECT
      tar.sales_funnel_target_id,
      tar.target_month,
      tar.kpi_name,
      CASE 
        WHEN LOWER(tar.crm_user_business_unit) = 'japan'
          THEN 'Japan'
        ELSE tar.crm_user_sales_segment
        END                 AS crm_user_sales_segment,
      tar.crm_user_sales_segment_grouped,
      tar.crm_user_geo,
      tar.crm_user_region,
      tar.crm_user_area,
      tar.crm_user_sales_segment_region_grouped,
      tar.order_type_name,
      tar.order_type_grouped,
      tar.sales_qualified_source_name,
      tar.sales_qualified_source_grouped,
      tar.created_by,
      tar.updated_by,
      tar.model_created_date,
      tar.model_updated_date,
      tar.dbt_updated_at,
      tar.dbt_created_at,
      SUM(tar.allocated_target) AS allocated_target

    FROM mart_sales_funnel_target tar
    -- FROM prod.restricted_safe_common_mart_sales.mart_sales_funnel_target
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

  ), mart_sales_funnel_target_expanded AS (

    SELECT
      funnel_target.*,
      -- 20220214 NF: Temporary keys, until the SFDC key is exposed,
      CASE
        WHEN funnel_target.order_type_name = '3. Growth'
            THEN '2. Growth'
        WHEN funnel_target.order_type_name = '1. New - First Order'
            THEN '1. New'
          ELSE '3. Other'
      END                                                AS deal_group,
      COALESCE(funnel_target.sales_qualified_source_name,'NA')                                              AS sales_qualified_source,
      LOWER(CONCAT(funnel_target.crm_user_sales_segment,'-',
          funnel_target.crm_user_geo,'-',
          funnel_target.crm_user_region,'-',
          funnel_target.crm_user_area, '-',
          sales_qualified_source, '-',
          deal_group)
          )                                                 AS report_user_segment_geo_region_area_sqs_ot
    FROM mart_sales_funnel_target_prep AS funnel_target

  ), final AS (

    SELECT
      funnel_target.*,
      target_month.fiscal_quarter_name_fy           AS target_fiscal_quarter_name,
      target_month.first_day_of_fiscal_quarter      AS target_fiscal_quarter_date,
      target_month.fiscal_year                      AS target_fiscal_year,

      agg_demo_keys.sales_team_cro_level,
      agg_demo_keys.sales_team_vp_level,
      agg_demo_keys.sales_team_avp_rd_level,
      agg_demo_keys.sales_team_asm_level,
      agg_demo_keys.sales_team_rd_asm_level,

      agg_demo_keys.key_segment,
      agg_demo_keys.key_sqs,
      agg_demo_keys.key_ot,

      agg_demo_keys.key_segment_geo,
      agg_demo_keys.key_segment_geo_sqs,
      agg_demo_keys.key_segment_geo_ot,

      agg_demo_keys.key_segment_geo_region,
      agg_demo_keys.key_segment_geo_region_sqs,
      agg_demo_keys.key_segment_geo_region_ot,

      agg_demo_keys.key_segment_geo_region_area,
      agg_demo_keys.key_segment_geo_region_area_sqs,
      agg_demo_keys.key_segment_geo_region_area_ot,
      agg_demo_keys.report_user_segment_geo_region_area

    FROM mart_sales_funnel_target_expanded funnel_target
    INNER JOIN  date_details target_month
      ON target_month.date_actual = funnel_target.target_month
    LEFT JOIN agg_demo_keys
      ON funnel_target.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys.report_user_segment_geo_region_area_sqs_ot
    WHERE LOWER(funnel_target.deal_group) LIKE ANY ('%growth%','%new%')

)

SELECT *
FROM final