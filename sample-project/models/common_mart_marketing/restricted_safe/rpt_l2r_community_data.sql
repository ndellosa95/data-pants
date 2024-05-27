{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('rpt_lead_to_revenue','rpt_lead_to_revenue'),
    ('dim_quote','dim_quote'),
    ('dim_date', 'dim_date'), 
    ('zuora_invoice_charges', 'zuora_invoice_charges'), 
    ('mart_crm_opportunity', 'mart_crm_opportunity'), 
    ('fct_quote_item','fct_quote_item')
]) }}

,lead_to_revenue AS (
  
    SELECT *
    FROM rpt_lead_to_revenue
    WHERE dim_crm_opportunity_id != '0068X00001Dioy1QAB' /*requested by c_hupy 2022-08-16 via slack*/
    AND dim_crm_opportunity_id NOT LIKE '0068X00001Dm8Jq%' /*requested by c_hupy 2023-01-10 via this issue https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/812#note_1232747975 */
    AND dim_crm_account_id NOT LIKE '0014M00001pdoUn%' --requested by bbehr 2023-02-20 via https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/1081

), date_details AS (
  
  SELECT *
  FROM dim_date
   
), dim_quote_base AS (

  SELECT
    dim_quote.dim_quote_id,
    dim_crm_opportunity_id,
    dim_subscription_id,
    is_primary_quote,
    quote_start_date,
    product_rate_plan_id,
    created_date,
    ROW_NUMBER() OVER (PARTITION BY dim_crm_opportunity_id ORDER BY created_date DESC) AS quote_number
  FROM dim_quote
  LEFT JOIN fct_quote_item
    ON fct_quote_item.dim_quote_id=dim_quote.dim_quote_id

), dim_quote_final AS (
  
  SELECT
    dim_quote_id,
    dim_crm_opportunity_id,
    dim_subscription_id,
    is_primary_quote,
    product_rate_plan_id,
    quote_start_date,
    created_date
  FROM dim_quote_base
  WHERE quote_number = 1
  
)

SELECT DISTINCT
--IDs
  lead_to_revenue.dim_crm_opportunity_id,
  lead_to_revenue.dim_crm_account_id,
  mart_crm_opportunity.dim_parent_crm_account_id,
  dim_quote_final.dim_subscription_id,
  lead_to_revenue.opp_dim_crm_user_id AS opportunity_owner_id,

--Opportunity Dimensions
  mart_crm_opportunity.opportunity_name,
  lead_to_revenue.crm_account_name,
  lead_to_revenue.opp_order_type,
  lead_to_revenue.sales_qualified_source_name,
  lead_to_revenue.sales_type,
  lead_to_revenue.opp_account_demographics_sales_segment,
  lead_to_revenue.opp_account_demographics_region,
  lead_to_revenue.opp_account_demographics_area,
  lead_to_revenue.opp_account_demographics_geo,
  lead_to_revenue.opp_account_demographics_territory,
  SPLIT_PART(TRIM(SPLIT_PART(mart_crm_opportunity.opportunity_name, '-', -1)), ' ', '1') AS seats_,
  TRY_TO_NUMERIC(TRIM(REPLACE(seats_, ',', ''))) AS seats,
  mart_crm_opportunity.product_category,
  CASE
    WHEN product_rate_plan_id IN ('2c92a00f76f0d5140176f2e349ee0a5a','2c92a00e76f0c6930176f2adada50acd','2c92a00f76f0d5180176f29f64092221','2c92a0fe76f0c6ab0176f2f671111254','2c92a0087a528186017a54696c1a762e','2c92a00f76f0d5140176f2e349ee0a5a','2c92a00f76f0d5180176f29f64092221')
        THEN 'EDU'
    WHEN product_rate_plan_id IN ('2c92a0ff76f0d5230176f2fcf4f93d95','2c92a00f76f0d5140176f2c3818954f7','2c92a00d76f0d5060176f2b951813c89','2c92a00876f0d4f80176f30016f21f91')
        THEN 'OSS'
    WHEN product_rate_plan_id IN ('2c92a011774f63f601775030a04b7d82','2c92a008774f63dd01775030b1d27a74','2c92a00f774f640601775032dece4852','2c92a00e774f54ab01775032eeb74fa5','2c92a00f774f640601775032dece4852','2c92a011774f63f601775030a04b7d82','2c92a008774f63dd01775030b1d27a74')
        THEN 'STARTUP'
    END AS community_data_type,

--Opporunity Created Dates
  opp_created_date.fiscal_year                     AS opportunity_created_date_range_year,
  opp_created_date.fiscal_quarter_name_fy          AS opportunity_created_date_range_quarter,
  opp_created_date.first_day_of_month              AS opportunity_created_date_range_month,
  opp_created_date.first_day_of_week               AS opportunity_created_date_range_week,
  opp_created_date.date_day                        AS opportunity_created_date_range_day,
  opp_created_date.date_id                         AS opportunity_created_date_range_id,

--Opporunity SAO Dates
  sao_date.fiscal_year                             AS sales_accepted_date_range_year,
  sao_date.fiscal_quarter_name_fy                  AS sales_accepted_date_range_quarter,
  sao_date.first_day_of_month                      AS sales_accepted_date_range_month,
  sao_date.first_day_of_week                       AS sales_accepted_date_range_week,
  sao_date.date_day                                AS sales_accepted_date_range_day,
  sao_date.date_id                                 AS sales_accepted_date_range_id,

--Opporunity Close Dates
  close_date.fiscal_year                           AS opportunity_closed_date_range_year,
  close_date.fiscal_quarter_name_fy                AS opportunity_closed_date_range_quarter,
  close_date.first_day_of_month                    AS opportunity_closed_date_range_month,
  close_date.first_day_of_week                     AS opportunity_closed_date_range_week,
  close_date.date_day                              AS opportunity_closed_date_range_day,
  close_date.date_id                               AS opportunity_closed_date_range_id,

--Opportunity Flags
  lead_to_revenue.is_sao,
  lead_to_revenue.is_won,
  lead_to_revenue.is_net_arr_closed_deal,
  lead_to_revenue.is_net_arr_pipeline_created,

--Zuora Fields
--   zuora_invoice_charges.effective_start_date, --replace with close_date
  zuora_invoice_charges.quantity, --
  zuora_invoice_charges.is_last_segment_version,
  CASE
    WHEN zuora_invoice_charges.quantity = 1 OR zuora_invoice_charges.quantity IS NULL THEN seats
    ELSE zuora_invoice_charges.quantity
  END AS number_of_seats,
  ROW_NUMBER() OVER(PARTITION BY mart_crm_opportunity.dim_parent_crm_account_id, lead_to_revenue.opp_order_type ORDER BY effective_start_date) AS subscription_order,
  IFF(subscription_order = 1 AND /*lead_to_revenue.opp_order_type = '1. New - First Order'*/lead_to_revenue.sales_type = 'New Business', TRUE, FALSE) AS is_first_subscription_institution
FROM lead_to_revenue
LEFT JOIN mart_crm_opportunity
  ON lead_to_revenue.dim_crm_opportunity_id=mart_crm_opportunity.dim_crm_opportunity_id
LEFT JOIN dim_quote_final
  ON lead_to_revenue.dim_crm_opportunity_id=dim_quote_final.dim_crm_opportunity_id
LEFT JOIN zuora_invoice_charges
  ON dim_quote_final.dim_subscription_id=zuora_invoice_charges.subscription_id
LEFT JOIN date_details opp_created_date
  ON lead_to_revenue.opp_created_date=opp_created_date.date_day
LEFT JOIN date_details sao_date
  ON lead_to_revenue.sales_accepted_date=sao_date.date_day
LEFT JOIN date_details close_date
  ON lead_to_revenue.close_date=close_date.date_day
WHERE (dim_quote_final.is_primary_quote = TRUE
     OR lead_to_revenue.sales_qualified_source_name= 'Web Direct Generated')
     AND community_data_type IS NOT NULL