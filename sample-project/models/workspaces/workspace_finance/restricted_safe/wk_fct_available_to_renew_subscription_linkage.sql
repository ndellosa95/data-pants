{{ config(
    materialized="table",
) }}

{{ simple_cte([
    ('wk_fct_available_to_renew', 'wk_fct_available_to_renew')
]) }}
 
-- ATR Calculation for all Quarters along with Renewal Linkage Subscriptions
, renewal_linkage AS ( 

    SELECT DISTINCT
      fiscal_quarter_name_fy, 
      dim_crm_account_id, 
      dim_crm_opportunity_id,
      dim_subscription_id, 
      subscription_name,
      LEAD(dim_subscription_id) OVER (PARTITION BY subscription_name ORDER BY atr_term_end_date) AS renewal_subscription_id,
      renewal_subscription_name,
      dim_billing_account_id,
      dim_parent_crm_account_id,
      parent_crm_account_name,
      ATR_term_start_date,
      ATR_term_end_date,
      dim_crm_user_id,
      user_name,
      crm_user_sales_segment,
      crm_user_geo,
      crm_user_region,
      crm_user_area,
      SUM(ARR) AS ARR, 
      SUM(Quantity) AS Quantity
    FROM  {{ ref('wk_fct_available_to_renew') }} 
    GROUP BY 1,2,3,4,5,7,8,9,10,11,12,13,14,15,16,17,18
)

{{ dbt_audit(
cte_ref="renewal_linkage",
created_by="@snalamaru",
updated_by="@snalamaru",
created_date="2024-04-01",
updated_date="2024-04-22"
) }}


