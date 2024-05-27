{{ simple_cte([
    ('fct_sales_funnel_partner_alliance_target', 'fct_sales_funnel_partner_alliance_target'),
    ('prep_date','prep_date')
]) }}

, daily_targets AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['fct_sales_funnel_partner_alliance_target.sales_funnel_partner_alliance_target_id', 'prep_date.date_day']) }}
                                                                                                                                  AS sales_funnel_partner_alliance_target_daily_pk,
      prep_date.date_day                                                                                                          AS target_date,
      prep_date.date_id                                                                                                           AS target_date_id,
      DATEADD('day', 1, target_date)                                                                                              AS report_target_date,
      DATEDIFF('day', fct_sales_funnel_partner_alliance_target.first_day_of_month, prep_date.last_day_of_month) + 1               AS days_of_month,
      prep_date.first_day_of_week,
      prep_date.fiscal_quarter_name,
      {{ dbt_utils.star(from=ref('fct_sales_funnel_partner_alliance_target'),
                        except=['CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_UPDATED_AT', 'DBT_CREATED_AT'],
                        relation_alias='fct_sales_funnel_partner_alliance_target') }},
      fct_sales_funnel_partner_alliance_target.allocated_target                                                                   AS monthly_allocated_target,
      fct_sales_funnel_partner_alliance_target.allocated_target / days_of_month                                                   AS daily_allocated_target
    FROM fct_sales_funnel_partner_alliance_target
    INNER JOIN prep_date
      ON fct_sales_funnel_partner_alliance_target.first_day_of_month = prep_date.first_day_of_month
    
), qtd_mtd_target AS (

    SELECT
      daily_targets.*,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, dim_crm_user_hierarchy_sk, dim_order_type_id, dim_alliance_type_id, dim_sales_qualified_source_id,
                              dim_partner_category_id, dim_channel_type_id, first_day_of_week ORDER BY target_date)               AS wtd_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, dim_crm_user_hierarchy_sk, dim_order_type_id, dim_alliance_type_id, dim_sales_qualified_source_id,
                              dim_partner_category_id, dim_channel_type_id, first_day_of_month ORDER BY target_date)              AS mtd_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, dim_crm_user_hierarchy_sk, dim_order_type_id, dim_alliance_type_id, dim_sales_qualified_source_id,
                              dim_partner_category_id, dim_channel_type_id, fiscal_quarter_name ORDER BY target_date)             AS qtd_allocated_target,
      SUM(daily_allocated_target) OVER(PARTITION BY kpi_name, dim_crm_user_hierarchy_sk, dim_order_type_id, dim_alliance_type_id, dim_sales_qualified_source_id,
                              dim_partner_category_id, dim_channel_type_id, fiscal_year ORDER BY target_date)                     AS ytd_allocated_target

    FROM daily_targets

)

{{ dbt_audit(
    cte_ref="qtd_mtd_target",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2023-09-20",
    updated_date="2023-10-27"
) }}
