{{ simple_cte([
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy'),
    ('dim_order_type','dim_order_type'),
    ('fct_sales_funnel_partner_alliance_target_daily', 'fct_sales_funnel_partner_alliance_target_daily'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('dim_alliance_type', 'dim_alliance_type_scd'),
    ('dim_channel_type', 'dim_channel_type'),
    ('dim_date','dim_date'),
    ('dim_partner_category', 'dim_partner_category')
]) }}

, targets AS (

    SELECT
      fct_sales_funnel_partner_alliance_target_daily.sales_funnel_partner_alliance_target_daily_pk    AS primary_key,
      fct_sales_funnel_partner_alliance_target_daily.target_date,
      fct_sales_funnel_partner_alliance_target_daily.report_target_date,
      fct_sales_funnel_partner_alliance_target_daily.first_day_of_week,
      fct_sales_funnel_partner_alliance_target_daily.first_day_of_month                               AS target_month,
      fct_sales_funnel_partner_alliance_target_daily.fiscal_quarter_name,
      fct_sales_funnel_partner_alliance_target_daily.fiscal_year,
      fct_sales_funnel_partner_alliance_target_daily.kpi_name,
      dim_crm_user_hierarchy.crm_user_sales_segment,
      dim_crm_user_hierarchy.crm_user_sales_segment_grouped,
      dim_crm_user_hierarchy.crm_user_business_unit,
      dim_crm_user_hierarchy.crm_user_geo,
      dim_crm_user_hierarchy.crm_user_region,
      dim_crm_user_hierarchy.crm_user_area,
      dim_crm_user_hierarchy.crm_user_sales_segment_region_grouped,
      dim_order_type.order_type_name,
      dim_order_type.order_type_grouped,
      dim_alliance_type.alliance_type_name,
      dim_alliance_type.alliance_type_short_name,
      dim_partner_category.partner_category_name,
      dim_sales_qualified_source.sales_qualified_source_name,
      dim_sales_qualified_source.sqs_bucket_engagement,
      dim_channel_type.channel_type_name,
      fct_sales_funnel_partner_alliance_target_daily.monthly_allocated_target,
      fct_sales_funnel_partner_alliance_target_daily.daily_allocated_target,
      fct_sales_funnel_partner_alliance_target_daily.wtd_allocated_target,
      fct_sales_funnel_partner_alliance_target_daily.mtd_allocated_target,
      fct_sales_funnel_partner_alliance_target_daily.qtd_allocated_target,
      fct_sales_funnel_partner_alliance_target_daily.ytd_allocated_target
    FROM fct_sales_funnel_partner_alliance_target_daily
    LEFT JOIN dim_alliance_type
      ON fct_sales_funnel_partner_alliance_target_daily.dim_alliance_type_id = dim_alliance_type.dim_alliance_type_id
    LEFT JOIN dim_sales_qualified_source
      ON fct_sales_funnel_partner_alliance_target_daily.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
    LEFT JOIN dim_channel_type
      ON fct_sales_funnel_partner_alliance_target_daily.dim_channel_type_id = dim_channel_type.dim_channel_type_id
    LEFT JOIN dim_order_type
      ON fct_sales_funnel_partner_alliance_target_daily.dim_order_type_id = dim_order_type.dim_order_type_id
    LEFT JOIN dim_crm_user_hierarchy
      ON fct_sales_funnel_partner_alliance_target_daily.dim_crm_user_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
    LEFT JOIN dim_partner_category
      ON fct_sales_funnel_partner_alliance_target_daily.dim_partner_category_id = dim_partner_category.dim_partner_category_id

)

{{ dbt_audit(
    cte_ref="targets",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2021-04-08",
    updated_date="2023-09-20",
  ) }}
