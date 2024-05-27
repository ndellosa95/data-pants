{{ config(alias='report_metrics_summary_upa_year') }}

WITH consolidated_accounts AS (

    SELECT *
    --FROM  prod.workspace_sales.report_metrics_summary_account_year
    FROM {{ ref('wk_sales_report_metrics_summary_account_year') }}

------------------------
   
), consolidated_upa AS (

  SELECT DISTINCT
    acc.report_fiscal_year,
    acc.virtual_upa_type            AS upa_type,
    acc.virtual_upa_id              AS upa_id,
    acc.virtual_upa_name            AS upa_name,
    acc.virtual_upa_owner_name      AS upa_owner_name,
    acc.virtual_upa_owner_id         AS upa_owner_id,
    ''                              AS upa_owner_title_category,
    acc.virtual_upa_industry        AS upa_industry,

    -- Account Demographics
    -- 20231025 as FY24, starting FY25 planning, this fields include the FY25 target

    acc.virtual_upa_ad_business_unit    AS upa_ad_business_unit,

    acc.virtual_upa_ad_segment          AS upa_ad_segment,
    acc.virtual_upa_ad_geo              AS upa_ad_geo,
    acc.virtual_upa_ad_region           AS upa_ad_region,
    acc.virtual_upa_ad_area             AS upa_ad_area,
    acc.virtual_upa_ad_country          AS upa_ad_country,
    acc.virtual_upa_ad_state_name       AS upa_ad_state_name,
    acc.virtual_upa_ad_state_code       AS upa_ad_state_code,
    acc.virtual_upa_ad_zip_code         AS upa_ad_zip_code,

    -- Account User Owner fields

    -- TODO add business unit and roletype
    -- this is wrong as this field should be taken from the account owner and not the
    -- account demographics field

    acc.virtual_upa_sub_business_unit   AS upa_user_business_unit,
    acc.virtual_upa_sub_business_unit   AS upa_user_sub_business_unit,
    acc.virtual_upa_division            AS upa_user_division,
    acc.virtual_upa_asm                 AS upa_user_asm,

    acc.virtual_upa_segment             AS upa_user_segment,
    acc.virtual_upa_geo                 AS upa_user_geo,
    acc.virtual_upa_region              AS upa_user_region,
    acc.virtual_upa_area                AS upa_user_area,

    acc.virtual_upa_country_name        AS upa_country,
    acc.virtual_upa_state_name          AS upa_state_name,
    acc.virtual_upa_state_code          AS upa_state_code,
    acc.virtual_upa_zip_code            AS upa_zip_code,

    acc.lam_dev_count_bin_rank,
    acc.lam_dev_count_bin_name,

    -- Public Sector
    CASE
        WHEN MAX(acc.is_public_sector_flag) = 1
            THEN 'Public'
        ELSE 'Private'
    END                             AS sector_type,

    -- Customer score used in maps for account visualization
    MAX(acc.customer_score) AS customer_score,

    MAX(acc.is_public_sector_flag)      AS is_public_sector_flag,


    -- SUM(CASE WHEN acc.account_forbes_rank IS NOT NULL THEN 1 ELSE 0 END)   AS count_forbes_accounts,
    -- MIN(account_forbes_rank)      AS forbes_rank,
    MAX(acc.potential_users)          AS potential_users,
    MAX(acc.licenses)                 AS licenses,
    MAX(acc.linkedin_developer)       AS linkedin_developer,
    MAX(acc.zi_developers)            AS zi_developers,
    MAX(acc.zi_revenue)               AS zi_revenue,
    MAX(acc.employees)                AS employees,
    MAX(acc.upa_lam_dev_count)        AS upa_lam_dev_count,

    -- atr for current fy
    SUM(acc.fy_atr)  AS fy_atr,
    -- next fiscal year atr base reported at fy
    SUM(acc.nfy_atr) AS nfy_atr,

    -- arr by fy
    SUM(acc.arr) AS arr,

    CASE
        WHEN  MAX(acc.is_customer_flag) = 1
        THEN 0
    ELSE 1
    END                                   AS is_prospect_flag,
    MAX(acc.is_customer_flag)             AS is_customer_flag,
    MAX(acc.is_over_5k_customer_flag)     AS is_over_5k_customer_flag,
    MAX(acc.is_over_10k_customer_flag)    AS is_over_10k_customer_flag,
    MAX(acc.is_over_50k_customer_flag)    AS is_over_50k_customer_flag,
    MAX(acc.is_over_500k_customer_flag)   AS is_over_500k_customer_flag,
    SUM(acc.is_over_5k_customer_flag)     AS count_over_5k_customers,
    SUM(acc.is_over_10k_customer_flag)    AS count_over_10k_customers,
    SUM(acc.is_over_50k_customer_flag)    AS count_over_50k_customers,
    SUM(acc.is_over_500k_customer_flag)   AS count_over_500k_customers,
    SUM(acc.is_prospect_flag)             AS count_of_prospects,
    SUM(acc.is_customer_flag)             AS count_of_customers,

    SUM(acc.arr_channel)                  AS arr_channel,
    SUM(acc.arr_direct)                   AS arr_direct,

    SUM(acc.product_starter_arr)          AS product_starter_arr,
    SUM(acc.product_premium_arr)          AS product_premium_arr,
    SUM(acc.product_ultimate_arr)         AS product_ultimate_arr,
    SUM(acc.delivery_self_managed_arr)    AS delivery_self_managed_arr,
    SUM(acc.delivery_saas_arr)            AS delivery_saas_arr,


    -- rolling last 12 months bokked net arr
    SUM(acc.last_12m_booked_net_arr)                      AS last_12m_booked_net_arr,
    SUM(acc.last_12m_booked_non_web_net_arr)              AS last_12m_booked_non_web_net_arr,
    SUM(acc.last_12m_booked_web_direct_sourced_net_arr)   AS last_12m_booked_web_direct_sourced_net_arr,
    SUM(acc.last_12m_booked_channel_sourced_net_arr)      AS last_12m_booked_channel_sourced_net_arr,
    SUM(acc.last_12m_booked_sdr_sourced_net_arr)          AS last_12m_booked_sdr_sourced_net_arr,
    SUM(acc.last_12m_booked_ae_sourced_net_arr)           AS last_12m_booked_ae_sourced_net_arr,
    SUM(acc.last_12m_booked_churn_contraction_net_arr)    AS last_12m_booked_churn_contraction_net_arr,
    SUM(acc.last_12m_booked_fo_net_arr)                   AS last_12m_booked_fo_net_arr,
    SUM(acc.last_12m_booked_new_connected_net_arr)        AS last_12m_booked_new_connected_net_arr,
    SUM(acc.last_12m_booked_growth_net_arr)               AS last_12m_booked_growth_net_arr,
    SUM(acc.last_12m_booked_deal_count)                   AS last_12m_booked_deal_count,
    SUM(acc.last_12m_booked_direct_net_arr)               AS last_12m_booked_direct_net_arr,
    SUM(acc.last_12m_booked_channel_net_arr)              AS last_12m_booked_channel_net_arr,
    SUM(acc.last_12m_atr)                                 AS last_12m_atr,

    -- fy booked net arr
    SUM(acc.fy_booked_net_arr)                   AS fy_booked_net_arr,
    SUM(acc.fy_booked_web_direct_sourced_net_arr) AS fy_booked_web_direct_sourced_net_arr,
    SUM(acc.fy_booked_channel_sourced_net_arr)   AS fy_booked_channel_sourced_net_arr,
    SUM(acc.fy_booked_sdr_sourced_net_arr)       AS fy_booked_sdr_sourced_net_arr,
    SUM(acc.fy_booked_ae_sourced_net_arr)        AS fy_booked_ae_sourced_net_arr,
    SUM(acc.fy_booked_churn_contraction_net_arr) AS fy_booked_churn_contraction_net_arr,
    SUM(acc.fy_booked_fo_net_arr)                AS fy_booked_fo_net_arr,
    SUM(acc.fy_booked_new_connected_net_arr)     AS fy_booked_new_connected_net_arr,
    SUM(acc.fy_booked_growth_net_arr)            AS fy_booked_growth_net_arr,
    SUM(acc.fy_booked_deal_count)                AS fy_booked_deal_count,
    SUM(acc.fy_booked_direct_net_arr)            AS fy_booked_direct_net_arr,
    SUM(acc.fy_booked_channel_net_arr)           AS fy_booked_channel_net_arr,
    SUM(acc.fy_booked_direct_deal_count)         AS fy_booked_direct_deal_count,
    SUM(acc.fy_booked_channel_deal_count)        AS fy_booked_channel_deal_count,

    -- open pipe forward looking
    SUM(acc.total_open_pipe)              AS total_open_pipe,
    SUM(acc.total_count_open_deals_pipe)  AS total_count_open_deals_pipe,
    SUM(acc.customer_has_open_pipe_flag)  AS customer_has_open_pipe_flag,
    SUM(acc.prospect_has_open_pipe_flag)  AS prospect_has_open_pipe_flag,

    -- pipe generation
    SUM(acc.pg_ytd_net_arr) AS pg_ytd_net_arr,
    SUM(acc.pg_ytd_web_direct_sourced_net_arr)    AS pg_ytd_web_direct_sourced_net_arr,
    SUM(acc.pg_ytd_channel_sourced_net_arr)       AS pg_ytd_channel_sourced_net_arr,
    SUM(acc.pg_ytd_sdr_sourced_net_arr)           AS pg_ytd_sdr_sourced_net_arr,
    SUM(acc.pg_ytd_ae_sourced_net_arr)            AS pg_ytd_ae_sourced_net_arr,

    SUM(acc.pg_last_12m_net_arr) AS pg_last_12m_net_arr,
    SUM(acc.pg_last_12m_web_direct_sourced_net_arr)   AS pg_last_12m_web_direct_sourced_net_arr,
    SUM(acc.pg_last_12m_channel_sourced_net_arr)      AS pg_last_12m_channel_sourced_net_arr,
    SUM(acc.pg_last_12m_sdr_sourced_net_arr)          AS pg_last_12m_sdr_sourced_net_arr,
    SUM(acc.pg_last_12m_ae_sourced_net_arr)           AS pg_last_12m_ae_sourced_net_arr,

    SUM(acc.last_12m_sao_deal_count)                    AS last_12m_sao_deal_count,
    SUM(acc.last_12m_sao_net_arr)                       AS last_12m_sao_net_arr,
    SUM(acc.last_12m_sao_booked_net_arr)                AS last_12m_sao_booked_net_arr,
    SUM(acc.fy_sao_deal_count)                          AS fy_sao_deal_count,
    SUM(acc.fy_sao_net_arr)                             AS fy_sao_net_arr,
    SUM(acc.fy_sao_booked_net_arr)                      AS fy_sao_booked_net_arr

  FROM consolidated_accounts acc
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31

), final AS (

    SELECT  upa.*
    FROM consolidated_upa upa

)

SELECT *
FROM final

