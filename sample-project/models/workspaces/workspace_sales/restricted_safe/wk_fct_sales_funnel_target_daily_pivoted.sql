{{ simple_cte([
    ('fct_sales_funnel_target_daily', 'wk_fct_sales_funnel_target_daily'),
    ('dim_date', 'dim_date')
    ])

}},

quarterly_targets AS (

  SELECT
      fiscal_quarter_name,
      kpi_name, 
      dim_crm_user_hierarchy_sk,
      dim_sales_qualified_source_id,
      dim_order_type_id,
      SUM(daily_allocated_target) AS quarterly_allocated_target
  FROM fct_sales_funnel_target_daily
  GROUP BY 1,2,3,4,5

),

quarterly_monthly_targets AS (

  SELECT
    fct_sales_funnel_target_daily.*,
    quarterly_targets.quarterly_allocated_target
  FROM fct_sales_funnel_target_daily
  LEFT JOIN quarterly_targets  
    ON fct_sales_funnel_target_daily.fiscal_quarter_name = quarterly_targets.fiscal_quarter_name
      AND fct_sales_funnel_target_daily.kpi_name = quarterly_targets.kpi_name
        AND fct_sales_funnel_target_daily.dim_crm_user_hierarchy_sk = quarterly_targets.dim_crm_user_hierarchy_sk
          AND fct_sales_funnel_target_daily.dim_sales_qualified_source_id = quarterly_targets.dim_sales_qualified_source_id
            AND fct_sales_funnel_target_daily.dim_order_type_id = quarterly_targets.dim_order_type_id
  
),

final AS (
  
  SELECT  
    {{ dbt_utils.generate_surrogate_key(['quarterly_monthly_targets.dim_crm_user_hierarchy_sk',
                                 'dim_date.fiscal_year', 
                                 'dim_date.first_day_of_month', 
                                 'quarterly_monthly_targets.dim_sales_qualified_source_id',
                                 'quarterly_monthly_targets.dim_order_type_id',
                                 'dim_date.date_day'
                                 ]) }}                                                                                  AS actuals_targets_pk,
    quarterly_monthly_targets.target_date, 
    quarterly_monthly_targets.target_date_id,
    quarterly_monthly_targets.report_target_date,
    quarterly_monthly_targets.dim_crm_user_hierarchy_sk,
    quarterly_monthly_targets.dim_order_type_id,
    quarterly_monthly_targets.dim_sales_qualified_source_id,
    quarterly_monthly_targets.days_of_month,
    quarterly_monthly_targets.first_day_of_week,
    quarterly_monthly_targets.fiscal_quarter_name,
    quarterly_monthly_targets.target_month_id, 
    quarterly_monthly_targets.first_day_of_month,
    quarterly_monthly_targets.fiscal_year,
    dim_date.day_of_week,
    dim_date.date_id,
    dim_date.fiscal_month_name_fy,
    dim_date.fiscal_quarter_name_fy,
    dim_date.first_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year,
    dim_date.last_day_of_week,
    dim_date.last_day_of_month,
    dim_date.last_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_year,
    -- CHURN % ATR
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN daily_allocated_target ELSE 0 END) AS churn_percentage_atr_daily_target,
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN monthly_allocated_target ELSE 0 END) AS churn_percentage_atr_monthly_target,
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN quarterly_allocated_target ELSE 0 END) AS churn_percentage_atr_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN wtd_allocated_target ELSE 0 END) AS churn_percentage_atr_wtd_target,
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN mtd_allocated_target ELSE 0 END) AS churn_percentage_atr_mtd_target,
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN qtd_allocated_target ELSE 0 END) AS churn_percentage_atr_qtd_target,
    MAX(CASE WHEN kpi_name = 'Churn % ATR' THEN ytd_allocated_target ELSE 0 END) AS churn_percentage_atr_ytd_target,

    -- CHURN AMOUNT
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN daily_allocated_target ELSE 0 END) AS churn_amount_daily_target,
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN monthly_allocated_target ELSE 0 END) AS churn_amount_monthly_target,
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN quarterly_allocated_target ELSE 0 END) AS churn_amount_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN wtd_allocated_target ELSE 0 END) AS churn_amount_wtd_target,
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN mtd_allocated_target ELSE 0 END) AS churn_amount_mtd_target,
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN qtd_allocated_target ELSE 0 END) AS churn_amount_qtd_target,
    MAX(CASE WHEN kpi_name = 'Churn Amount' THEN ytd_allocated_target ELSE 0 END) AS churn_amount_ytd_target,

    -- CHURN/CONTRACTION AMOUNT
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN daily_allocated_target ELSE 0 END) AS churn_contraction_amount_daily_target,
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN monthly_allocated_target ELSE 0 END) AS churn_contraction_amount_monthly_target,
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN quarterly_allocated_target ELSE 0 END) AS churn_contraction_amount_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN wtd_allocated_target ELSE 0 END) AS churn_contraction_amount_wtd_target,
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN mtd_allocated_target ELSE 0 END) AS churn_contraction_amount_mtd_target,
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN qtd_allocated_target ELSE 0 END) AS churn_contraction_amount_qtd_target,
    MAX(CASE WHEN kpi_name = 'Churn/Contraction Amount' THEN ytd_allocated_target ELSE 0 END) AS churn_contraction_amount_ytd_target,

    -- CONTRACTION % ATR
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN daily_allocated_target ELSE 0 END) AS contraction_percentage_atr_daily_target,
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN monthly_allocated_target ELSE 0 END) AS contraction_percentage_atr_monthly_target,
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN quarterly_allocated_target ELSE 0 END) AS contraction_percentage_atr_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN wtd_allocated_target ELSE 0 END) AS contraction_percentage_atr_wtd_target,
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN mtd_allocated_target ELSE 0 END) AS contraction_percentage_atr_mtd_target,
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN qtd_allocated_target ELSE 0 END) AS contraction_percentage_atr_qtd_target,
    MAX(CASE WHEN kpi_name = 'Contraction % ATR' THEN ytd_allocated_target ELSE 0 END) AS contraction_percentage_atr_ytd_target,

    -- CONTRACTION AMOUNT
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN daily_allocated_target ELSE 0 END) AS contraction_amount_daily_target,
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN monthly_allocated_target ELSE 0 END) AS contraction_amount_monthly_target,
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN quarterly_allocated_target ELSE 0 END) AS contraction_amount_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN wtd_allocated_target ELSE 0 END) AS contraction_amount_wtd_target,
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN mtd_allocated_target ELSE 0 END) AS contraction_amount_mtd_target,
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN qtd_allocated_target ELSE 0 END) AS contraction_amount_qtd_target,
    MAX(CASE WHEN kpi_name = 'Contraction Amount' THEN ytd_allocated_target ELSE 0 END) AS contraction_amount_ytd_target,

    -- DEALS
    MAX(CASE WHEN kpi_name = 'Deals' THEN daily_allocated_target ELSE 0 END) AS deals_daily_target,
    MAX(CASE WHEN kpi_name = 'Deals' THEN monthly_allocated_target ELSE 0 END) AS deals_monthly_target,
    MAX(CASE WHEN kpi_name = 'Deals' THEN quarterly_allocated_target ELSE 0 END) AS deals_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Deals' THEN wtd_allocated_target ELSE 0 END) AS deals_wtd_target,
    MAX(CASE WHEN kpi_name = 'Deals' THEN mtd_allocated_target ELSE 0 END) AS deals_mtd_target,
    MAX(CASE WHEN kpi_name = 'Deals' THEN qtd_allocated_target ELSE 0 END) AS deals_qtd_target,
    MAX(CASE WHEN kpi_name = 'Deals' THEN ytd_allocated_target ELSE 0 END) AS deals_ytd_target,
    
    -- EXPANSION % ATR 
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN daily_allocated_target ELSE 0 END) AS expansion_percentage_atr_daily_target,
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN monthly_allocated_target ELSE 0 END) AS expansion_percentage_atr_monthly_target,
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN quarterly_allocated_target ELSE 0 END) AS expansion_percentage_atr_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN wtd_allocated_target ELSE 0 END) AS expansion_percentage_atr_wtd_target,
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN mtd_allocated_target ELSE 0 END) AS expansion_percentage_atr_mtd_target,
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN qtd_allocated_target ELSE 0 END) AS expansion_percentage_atr_qtd_target,
    MAX(CASE WHEN kpi_name = 'Expansion % ATR' THEN ytd_allocated_target ELSE 0 END) AS expansion_percentage_atr_ytd_target,

    -- EXPANSION AMOUNT
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN daily_allocated_target ELSE 0 END) AS expansion_amount_daily_target,
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN monthly_allocated_target ELSE 0 END) AS expansion_amount_monthly_target,
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN quarterly_allocated_target ELSE 0 END) AS expansion_amount_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN wtd_allocated_target ELSE 0 END) AS expansion_amount_wtd_target,
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN mtd_allocated_target ELSE 0 END) AS expansion_amount_mtd_target,
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN qtd_allocated_target ELSE 0 END) AS expansion_amount_qtd_target,
    MAX(CASE WHEN kpi_name = 'Expansion Amount' THEN ytd_allocated_target ELSE 0 END) AS expansion_amount_ytd_target,

    -- HIGH LAM STAGE CLOSED WON OPPORTUNITIES
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN daily_allocated_target ELSE 0 END) AS high_lam_stage_closed_won_opportunities_daily_target,
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN monthly_allocated_target ELSE 0 END) AS high_lam_stage_closed_won_opportunities_monthly_target,
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN quarterly_allocated_target ELSE 0 END) AS high_lam_stage_closed_won_opportunities_quarterly_target,
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN wtd_allocated_target ELSE 0 END) AS high_lam_stage_closed_won_opportunities_wtd_target,
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN mtd_allocated_target ELSE 0 END) AS high_lam_stage_closed_won_opportunities_mtd_target,
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN qtd_allocated_target ELSE 0 END) AS high_lam_stage_closed_won_opportunities_qtd_target,
    MAX(CASE WHEN kpi_name = 'High LAM Stage Closed Won Opportunities' THEN ytd_allocated_target ELSE 0 END) AS high_lam_stage_closed_won_opportunities_ytd_target,

    -- MQL
    MAX(CASE WHEN kpi_name = 'MQL' THEN daily_allocated_target ELSE 0 END) AS mql_daily_target,
    MAX(CASE WHEN kpi_name = 'MQL' THEN monthly_allocated_target ELSE 0 END) AS mql_monthly_target,
    MAX(CASE WHEN kpi_name = 'MQL' THEN quarterly_allocated_target ELSE 0 END) AS mql_quarterly_target,
    MAX(CASE WHEN kpi_name = 'MQL' THEN wtd_allocated_target ELSE 0 END) AS mql_wtd_target,
    MAX(CASE WHEN kpi_name = 'MQL' THEN mtd_allocated_target ELSE 0 END) AS mql_mtd_target,
    MAX(CASE WHEN kpi_name = 'MQL' THEN qtd_allocated_target ELSE 0 END) AS mql_qtd_target,
    MAX(CASE WHEN kpi_name = 'MQL' THEN ytd_allocated_target ELSE 0 END) AS mql_ytd_target,

    -- NET ARR
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN daily_allocated_target ELSE 0 END) AS net_arr_daily_target,
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN monthly_allocated_target ELSE 0 END) AS net_arr_monthly_target,
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN quarterly_allocated_target ELSE 0 END) AS net_arr_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN wtd_allocated_target ELSE 0 END) AS net_arr_wtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN mtd_allocated_target ELSE 0 END) AS net_arr_mtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN qtd_allocated_target ELSE 0 END) AS net_arr_qtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR' THEN ytd_allocated_target ELSE 0 END) AS net_arr_ytd_target,

    --NET ARR COMPANY
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN daily_allocated_target ELSE 0 END) AS net_arr_company_daily_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN monthly_allocated_target ELSE 0 END) AS net_arr_company_monthly_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN quarterly_allocated_target ELSE 0 END) AS net_arr_company_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN wtd_allocated_target ELSE 0 END) AS net_arr_company_wtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN mtd_allocated_target ELSE 0 END) AS net_arr_company_mtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN qtd_allocated_target ELSE 0 END) AS net_arr_company_qtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Company' THEN ytd_allocated_target ELSE 0 END) AS net_arr_company_ytd_target,

    -- NET ARR PIPELINE CREATED
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN daily_allocated_target ELSE 0 END) AS net_arr_pipeline_created_daily_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN monthly_allocated_target ELSE 0 END) AS net_arr_pipeline_created_monthly_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN quarterly_allocated_target ELSE 0 END) AS net_arr_pipeline_created_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN wtd_allocated_target ELSE 0 END) AS net_arr_pipeline_created_wtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN mtd_allocated_target ELSE 0 END) AS net_arr_pipeline_created_mtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN qtd_allocated_target ELSE 0 END) AS net_arr_pipeline_created_qtd_target,
    MAX(CASE WHEN kpi_name = 'Net ARR Pipeline Created' THEN ytd_allocated_target ELSE 0 END) AS net_arr_pipeline_created_ytd_target,

    -- NEW LOGOS
    MAX(CASE WHEN kpi_name = 'New Logos' THEN daily_allocated_target ELSE 0 END) AS new_logos_daily_target,
    MAX(CASE WHEN kpi_name = 'New Logos' THEN monthly_allocated_target ELSE 0 END) AS new_logos_monthly_target,
    MAX(CASE WHEN kpi_name = 'New Logos' THEN quarterly_allocated_target ELSE 0 END) AS new_logos_quarterly_target,
    MAX(CASE WHEN kpi_name = 'New Logos' THEN wtd_allocated_target ELSE 0 END) AS new_logos_wtd_target,
    MAX(CASE WHEN kpi_name = 'New Logos' THEN mtd_allocated_target ELSE 0 END) AS new_logos_mtd_target,
    MAX(CASE WHEN kpi_name = 'New Logos' THEN qtd_allocated_target ELSE 0 END) AS new_logos_qtd_target,
    MAX(CASE WHEN kpi_name = 'New Logos' THEN ytd_allocated_target ELSE 0 END) AS new_logos_ytd_target,
     
    -- PRO SERVE AMOUNT
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN daily_allocated_target ELSE 0 END) AS proserve_amount_daily_target,
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN monthly_allocated_target ELSE 0 END) AS proserve_amount_monthly_target,
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN quarterly_allocated_target ELSE 0 END) AS proserve_amount_quarterly_target,
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN wtd_allocated_target ELSE 0 END) AS proserve_amount_wtd_target,
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN mtd_allocated_target ELSE 0 END) AS proserve_amount_mtd_target,
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN qtd_allocated_target ELSE 0 END) AS proserve_amount_qtd_target,
    MAX(CASE WHEN kpi_name = 'ProServe Amount' THEN ytd_allocated_target ELSE 0 END) AS proserve_amount_ytd_target,

    -- STAGE 1 OPPORTUNITIES
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN daily_allocated_target ELSE 0 END) AS stage_1_opportunities_daily_target,
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN monthly_allocated_target ELSE 0 END) AS stage_1_opportunities_monthly_target,
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN quarterly_allocated_target ELSE 0 END) AS stage_1_opportunities_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN wtd_allocated_target ELSE 0 END) AS stage_1_opportunities_wtd_target,
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN mtd_allocated_target ELSE 0 END) AS stage_1_opportunities_mtd_target,
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN qtd_allocated_target ELSE 0 END) AS stage_1_opportunities_qtd_target,
    MAX(CASE WHEN kpi_name = 'Stage 1 Opportunities' THEN ytd_allocated_target ELSE 0 END) AS stage_1_opportunities_ytd_target,

    -- TOTAL CLOSED
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN daily_allocated_target ELSE 0 END) AS total_closed_daily_target,
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN monthly_allocated_target ELSE 0 END) AS total_closed_monthly_target,
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN quarterly_allocated_target ELSE 0 END) AS total_closed_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN wtd_allocated_target ELSE 0 END) AS total_closed_wtd_target,
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN mtd_allocated_target ELSE 0 END) AS total_closed_mtd_target,
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN qtd_allocated_target ELSE 0 END) AS total_closed_qtd_target,
    MAX(CASE WHEN kpi_name = 'Total Closed' THEN ytd_allocated_target ELSE 0 END) AS total_closed_ytd_target,
 
    -- TRIALS
    MAX(CASE WHEN kpi_name = 'Trials' THEN daily_allocated_target ELSE 0 END) AS trials_daily_target,
    MAX(CASE WHEN kpi_name = 'Trials' THEN monthly_allocated_target ELSE 0 END) AS trials_monthly_target,
    MAX(CASE WHEN kpi_name = 'Trials' THEN quarterly_allocated_target ELSE 0 END) AS trials_quarterly_target,
    MAX(CASE WHEN kpi_name = 'Trials' THEN wtd_allocated_target ELSE 0 END) AS trials_wtd_target,
    MAX(CASE WHEN kpi_name = 'Trials' THEN mtd_allocated_target ELSE 0 END) AS trials_mtd_target,
    MAX(CASE WHEN kpi_name = 'Trials' THEN qtd_allocated_target ELSE 0 END) AS trials_qtd_target,
    MAX(CASE WHEN kpi_name = 'Trials' THEN ytd_allocated_target ELSE 0 END) AS trials_ytd_target
FROM quarterly_monthly_targets
LEFT JOIN dim_date
  ON quarterly_monthly_targets.target_date = dim_date.date_actual
{{ dbt_utils.group_by(n=23)}} 

)

SELECT * 
FROM final
