{{ config(alias='report_metrics_summary_account_year') }}

WITH RECURSIVE date_details AS (

    SELECT *
    --FROM  prod.workspace_sales.date_details
    FROM {{ ref('wk_sales_date_details') }}

 ), sfdc_opportunity_xf AS (

    SELECT *
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_xf
    FROM {{ref('wk_sales_sfdc_opportunity_xf')}}
    WHERE is_deleted = 0
      AND is_edu_oss = 0
      AND is_jihu_account = 0

 ), sfdc_opportunity_snapshot_xf AS (

    SELECT h.*
    --FROM prod.restricted_safe_workspace_sales.sfdc_opportunity_snapshot_history_xf AS h
    FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}} h
    INNER JOIN date_details snapshot_date
      ON snapshot_date.date_actual = h.snapshot_date
    WHERE h.is_deleted = 0
      AND h.is_edu_oss = 0
      AND h.is_jihu_account = 0
      -- same day of FY across years
      AND snapshot_date.day_of_fiscal_year_normalised = (SELECT DISTINCT day_of_fiscal_year_normalised
                                                          FROM date_details
                                                          WHERE date_actual = DATEADD(day, -2, CURRENT_DATE))
 ), mart_available_to_renew AS (

    SELECT renew.*,
        renew_date.first_day_of_fiscal_quarter  AS renew_fiscal_quarter_date,
        renew_date.fiscal_quarter_name_fy       AS renew_fiscal_quarter_name,
        renew_date.fiscal_quarter               AS renew_fiscal_quarter_number
    --FROM {{ref('mart_available_to_renew')}} renew
    FROM prod.restricted_safe_common_mart_finance.mart_available_to_renew renew
    LEFT JOIN date_details renew_date
        ON renew_date.date_actual = renew.renewal_month

 ), dim_subscription AS (

    SELECT
      dim_subscription_id,
      CASE
          WHEN dim_billing_account_id_invoice_owner_account != dim_billing_account_id
              THEN 1
          ELSE 0
      END AS is_channel_arr_flag
    --FROM prod.common.dim_subscription
    FROM {{ ref('dim_subscription') }}

 ---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
---------- Base Account
 ),  sfdc_users_xf AS (

    SELECT user_id,
           name,
           department,
           title,
           team,
           user_email,
           manager_name,
           manager_id,
           user_geo,
           user_region,
           user_segment,
           raw_user_segment,
           adjusted_user_segment,
           user_area,
           role_name,
           role_type,
           start_date,
           is_active,
           is_hybrid_flag,
           employee_number,
           crm_user_business_unit,
           is_rep_flag,
           business_unit,
           sub_business_unit,
           division,
           asm,
           key_bu,
           key_bu_subbu,
           key_bu_subbu_division,
           key_bu_subbu_division_asm,
           key_sal_heatmap,
           CASE
            WHEN lower(title) like '%strategic account%'
                OR lower(title) like '%account executive%'
                OR lower(title) like '%country manager%'
                OR lower(title) like '%public sector channel manager%'
                THEN 'Rep'
            WHEN lower(title) like '%area sales manager%'
                THEN 'ASM'
            ELSE 'Other'
      END                                       AS title_category
    --FROM prod.workspace_sales.sfdc_users_xf sfdc_user
    FROM {{ref('wk_sales_sfdc_users_xf')}}

), raw_account AS (

    SELECT *
    FROM {{ source('salesforce', 'account') }}
    --FROM raw.salesforce_v2_stitch.account

 ), mart_crm_account AS (

    SELECT acc.*

    --FROM prod.restricted_safe_common_mart_sales.mart_crm_account acc
    FROM {{ref('mart_crm_account')}} acc

 ), mart_arr AS (

    SELECT *
    --FROM prod.restricted_safe_common_mart_sales.mart_arr
    FROM {{ref('mart_arr')}}

), arr_report_month AS (

    SELECT DISTINCT
        fiscal_year        AS report_fiscal_year,
        first_day_of_month AS report_month_date
    FROM date_details
    CROSS JOIN (SELECT DATEADD(MONTH, -1, DATEADD(day, -2, CURRENT_DATE)) AS today_date)
    WHERE date_actual = today_date

),last_arr_report AS (

    SELECT
        dim_crm_account_id AS account_id,
        report_month_date  AS arr_report_month,
        SUM(arr)           AS arr,
        SUM(quantity)      AS seats
    FROM mart_arr
    CROSS JOIN arr_report_month
    WHERE mart_arr.arr_month = arr_report_month.report_month_date
    GROUP BY 1, 2


),

base_account AS (

   SELECT

        raw_acc.has_tam__c                              AS has_tam_flag,
        raw_acc.public_sector_account__c                AS public_sector_account_flag,
        raw_acc.pubsec_type__c                          AS pubsec_type,
        -- For segment calculation we leverage the upa logic
        raw_upa.pubsec_type__c                          AS upa_pubsec_type,
        raw_acc.lam_tier__c                             AS lam_arr,
        raw_acc.lam_dev_count__c                        AS lam_dev_count,
        raw_upa.lam_tier__c                             AS upa_lam_arr,
        raw_upa.lam_dev_count__c                        AS upa_lam_dev_count,

        -- hierarchy industry is used as it identifies the most common industry in a hierarchy and it is used for routing
        -- of accounts
        raw_acc.parent_lam_industry_acct_heirarchy__c   AS account_industry,
        raw_upa.parent_lam_industry_acct_heirarchy__c   AS upa_industry,
        acc.dim_parent_crm_account_id                  AS upa_id,
        acc.dim_crm_account_id                         AS account_id,
        acc.crm_account_name                           AS account_name,

        upa_owner.user_id                              AS upa_owner_id,
        upa_owner.name                                 AS upa_owner_name,
        raw_upa.name                                   AS upa_name,
        UPPER(upa_owner.user_geo)                      AS upa_owner_geo,
        upa_owner.business_unit                        AS upa_owner_business_unit,

        upa_owner.sub_business_unit                    AS upa_owner_sub_bu,
        upa_owner.division                             AS upa_owner_division,
        upa_owner.asm                                  AS upa_owner_asm,

        upa_owner.user_region                          AS upa_owner_region,
        upa_owner.user_area                            AS upa_owner_area,
        upa_owner.user_segment                         AS upa_owner_segment,

        acc_owner.user_id                              AS account_owner_id,
        acc_owner.name                                 AS account_owner_name,
        UPPER(acc_owner.user_geo)                      AS account_owner_geo,
        acc_owner.business_unit                        AS account_owner_business_unit,

        acc_owner.sub_business_unit                    AS account_owner_sub_bu,
        acc_owner.division                             AS account_owner_division,
        acc_owner.asm                                  AS account_owner_asm,

        acc_owner.user_region                          AS account_owner_region,
        acc_owner.user_area                            AS account_owner_area,
        acc_owner.user_segment                         AS account_owner_segment,

        raw_acc.billingstate                            AS account_billing_state_name,
        raw_acc.billingstatecode                        AS account_billing_state_code,
        raw_acc.billingcountry                          AS account_billing_country_name,
        raw_acc.billingcountrycode                      AS account_billing_country_code,
        raw_acc.billingcity                             AS account_billing_city,
        raw_acc.billingpostalcode                       AS account_billing_postal_code,
        raw_acc.parent_lam_industry_acct_heirarchy__c   AS hierarcy_industry,

        account_arr.arr_report_month,
        account_arr.arr,

        NULL                                            AS is_key_account,
        acc.abm_tier,
        acc.health_score_color                          AS account_health_score_color,
        acc.health_number                               AS account_health_number,
        raw_acc.parentid                               AS parent_id,

        -- this fields might not be upa level
        raw_acc.account_demographics_business_unit__c       AS account_demographics_business_unit,
        UPPER(raw_acc.account_demographics_geo__c)          AS account_demographics_geo,
        raw_acc.account_demographics_region__c              AS account_demographics_region,
        raw_acc.account_demographics_area__c                AS account_demographics_area,
        UPPER(raw_acc.account_demographics_sales_segment__c) AS account_demographics_sales_segment,
        raw_acc.account_demographics_territory__c           AS account_demographics_territory,

        raw_upa.account_demographics_business_unit__c       AS account_demographics_upa_business_unit,
        UPPER(raw_upa.account_demographics_geo__c)          AS account_demographics_upa_geo,
        raw_upa.account_demographics_region__c              AS account_demographics_upa_region,
        raw_upa.account_demographics_area__c                AS account_demographics_upa_area,
        UPPER(raw_upa.account_demographics_sales_segment__c) AS account_demographics_upa_sales_segment,
        raw_upa.account_demographics_territory__c           AS account_demographics_upa_territory,

        raw_upa.account_demographics_upa_state__c           AS account_demographics_upa_state_code,
        raw_upa.account_demographics_upa_state_name__c      AS account_demographics_upa_state_name,
        raw_upa.account_demographics_upa_country_name__c    AS account_demographics_upa_country_name,
        raw_upa.account_demographics_upa_country__c         AS account_demographics_upa_country_code,
        raw_upa.account_demographics_upa_city__c            AS account_demographics_upa_city,
        raw_upa.account_demographics_upa_postal_code__c     AS account_demographics_upa_postal_code,
        raw_upa.account_demographic_max_family_employees__c AS account_demographics_upa_max_family_employees,


        -- fields from mart account
        --mart_crm_account.public_sector_account_flag,
        raw_acc.customer_score__c                    AS customer_score,


    COALESCE(raw_acc.potential_users__c,0)                      AS potential_users,
    COALESCE(raw_acc.number_of_licenses_this_account__c,0)      AS licenses,
    COALESCE(raw_acc.decision_maker_count_linkedin__c,0)        AS linkedin_developer,
    COALESCE(raw_acc.zi_number_of_developers__c,0)              AS zi_developers,
    COALESCE(raw_acc.zi_revenue__c,0)                           AS zi_revenue,
    COALESCE(raw_acc.account_demographics_employee_count__c,0)  AS employees,
    COALESCE(raw_acc.carr_acct_family__c,0)                     AS account_family_arr,
    LEAST(50000,GREATEST(COALESCE(raw_acc.number_of_licenses_this_account__c,0),COALESCE(raw_acc.potential_users__c, raw_acc.decision_maker_count_linkedin__c , raw_acc.zi_number_of_developers__c, 0)))           AS calculated_developer_count

    FROM mart_crm_account AS acc
    INNER JOIN raw_account AS raw_acc
        ON raw_acc.id = acc.dim_crm_account_id
    -- upa account demographics fields
    LEFT JOIN raw_account AS raw_upa
        ON raw_upa.id = acc.dim_parent_crm_account_id
    LEFT JOIN sfdc_users_xf AS acc_owner
        ON raw_acc.ownerid = acc_owner.user_id
    -- upa owner id doesn't seem to be on mart crm
    LEFT JOIN sfdc_users_xf AS upa_owner
        ON raw_upa.ownerid = upa_owner.user_id
    -- arr
    LEFT JOIN last_arr_report AS account_arr
        ON acc.dim_crm_account_id = account_arr.account_id
    -- NF: 20231102 - Remove disqualified accounts for FY24 - Brought up by Melia
    -- Excluding territories that are null
    WHERE COALESCE(raw_acc.account_demographics_territory__c,'')   NOT IN ('xDISQUALIFIED ACCOUNTS_','xDO NOT DO BUSINESS_',
                                                              'xCHANNEL','xUNKNOWN','xJIHU', 'JIHU',
                                                             'JIHU_JIHU_JIHU_JIHU_JIHU_JIHU_JIHU',
                                                             'UNKNOWN_UNKNOWN_UNKNOWN_UNKNOWN_UNKNOWN_UNKNOWN_UNKNOWN',
                                                              ''
                                                             )
    -- NF: Exclude partner accounts
        AND LOWER(raw_acc.type) != 'partner'
    -- NF: Remove duplicates, we are workign directly with source SFDC
        AND raw_acc.isdeleted = 0
     -- NF: 20231122 Remove PubSec
        AND COALESCE(lower(raw_acc.pubsec_type__c),'') !='us-pubsec'
-----------------------
-- Adjust for hierarchies split between different geos
-- Some of this accounts might have different owners
--   e.g 0014M00001gWR5vQAG
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

), wk_sales_report_bob_virtual_hierarchy AS (

    SELECT *
    --FROM {{ref('wk_sales_report_bob_virtual_hierarchy')}}
    FROM prod.restricted_safe_workspace_sales.report_bob_virtual_hierarchy

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

  ), report_dates AS (

    SELECT DISTINCT fiscal_year         AS report_fiscal_year,
                    first_day_of_month  AS report_month_date
    FROM prod.workspace_sales.date_details
    CROSS JOIN (SELECT  DATEADD(day, -2, CURRENT_DATE) AS today_date)
    WHERE fiscal_year > 2021
        AND month_actual = MONTH(today_date)
        AND date_actual < today_date

  ), account_year_key AS (

    SELECT DISTINCT
      a.account_id,
      d.report_fiscal_year,
      d.report_month_date
  FROM base_account AS a
  CROSS JOIN report_dates AS d

  ), nfy_atr_base AS (

    SELECT
        dim_crm_account_id      AS account_id,
        report_dates.report_fiscal_year,
        SUM(arr)                AS nfy_atr,
         SUM(CASE
          WHEN atr.renew_fiscal_quarter_number = 1
            THEN arr
          ELSE 0
        END)                    AS nfy_q1_atr,
        SUM(CASE
          WHEN atr.renew_fiscal_quarter_number = 2
            THEN arr
          ELSE 0
        END)                    AS nfy_q2_atr,
        SUM(CASE
          WHEN atr.renew_fiscal_quarter_number = 3
            THEN arr
          ELSE 0
        END)                    AS nfy_q3_atr,
        SUM(CASE
          WHEN atr.renew_fiscal_quarter_number = 4
            THEN arr
          ELSE 0
        END)                    AS nfy_q4_atr
    FROM mart_available_to_renew atr
    CROSS JOIN report_dates
    WHERE is_available_to_renew = 1
    AND atr.fiscal_year = report_dates.report_fiscal_year + 1
    GROUP BY 1,2

), last_12m_atr_base AS (

    SELECT dim_crm_account_id   AS account_id,
        report_dates.report_fiscal_year,
        COUNT(DISTINCT atr.renewal_month) AS count_unique_months,

        SUM(arr)                AS last_12m_atr
    FROM mart_available_to_renew atr
    CROSS JOIN report_dates
    WHERE is_available_to_renew = 1
    --AND renewal_type = 'Non-MYB'
    AND atr.renewal_month < report_dates.report_month_date
    AND atr.renewal_month >= DATEADD(month,-12,report_dates.report_month_date)
    GROUP BY 1,2

), fy_atr_base AS (

    SELECT dim_crm_account_id   AS account_id,
        report_dates.report_fiscal_year,
        COUNT(DISTINCT atr.renewal_month) AS count_unique_months,
        SUM(arr)                AS fy_atr
    FROM mart_available_to_renew atr
    CROSS JOIN report_dates
    WHERE is_available_to_renew = 1
    --AND renewal_type = 'Non-MYB'
    AND atr.fiscal_year = report_dates.report_fiscal_year
    GROUP BY 1,2


-- Rolling 1 year Net ARR
), net_arr_last_12m AS (
  -- net_arr_ttm

    SELECT
      o.account_id,
      d.report_fiscal_year          AS report_fiscal_year,
      SUM(o.net_arr)                AS last_12m_booked_net_arr,   -- ttm_net_arr
      SUM(CASE
            WHEN  o.sales_qualified_source != 'Web Direct Generated'
              THEN o.net_arr
            ELSE 0
          END)          AS last_12m_booked_non_web_net_arr,  -- ttm_non_web_net_arr
      SUM(CASE
            WHEN o.sales_qualified_source = 'Web Direct Generated'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_web_direct_sourced_net_arr,  --ttm_web_direct_sourced_net_arr
      SUM(CASE
            WHEN (o.sales_qualified_source = 'Partner Generated' OR o.sales_qualified_source = 'Partner Generated')
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_channel_sourced_net_arr,  -- ttm_web_direct_sourced_net_arr
      SUM(CASE
            WHEN o.sales_qualified_source = 'SDR Generated'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_sdr_sourced_net_arr,  -- ttm_sdr_sourced_net_arr
      SUM(CASE
            WHEN o.sales_qualified_source = 'AE Generated'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_ae_sourced_net_arr,  -- ttm_ae_sourced_net_arr
      SUM(CASE
            WHEN o.is_eligible_churn_contraction_flag = 1
               THEN o.booked_churned_contraction_net_arr
            ELSE 0 END) AS last_12m_booked_churn_contraction_net_arr,  -- ttm_churn_contraction_net_arr

       -- FO year
        SUM(CASE
            WHEN o.order_type_stamped = '1. New - First Order'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_fo_net_arr,  -- ttm_fo_net_arr

        -- New Connected year
        SUM(CASE
            WHEN o.order_type_stamped = '2. New - Connected'
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_new_connected_net_arr, -- ttm_new_connected_net_arr

        -- Growth year
        SUM(CASE
            WHEN o.order_type_stamped NOT IN ('2. New - Connected','1. New - First Order')
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_growth_net_arr,   --ttm_growth_net_arr

        -- deal path direct year
        SUM(CASE
            WHEN (o.deal_path != 'Channel' AND o.deal_path != 'Partner')
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_direct_net_arr,   --ttm_direct_net_arr

        -- deal path channel year
        SUM(CASE
            WHEN (o.deal_path = 'Channel' OR o.deal_path = 'Partner')
            THEN o.net_arr
            ELSE 0 END) AS last_12m_booked_channel_net_arr,   --ttm_channel_net_arr

        SUM (CASE
            WHEN o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_deal_count,  --ttm_deal_count

         SUM (CASE
            WHEN (o.is_won = 1
                  OR (o.is_renewal = 1 AND o.is_lost = 1))
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_trx_count,  -- ttm_trx_count

          SUM (CASE
            WHEN (o.is_won = 1
                  OR (o.is_renewal = 1 AND o.is_lost = 1))
                AND ((o.is_renewal = 1 AND o.arr_basis > 5000)
                        OR o.net_arr > 5000)
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_trx_over_5k_count,   -- ttm_trx_over_5k_count

          SUM (CASE
            WHEN (o.is_won = 1
                  OR (o.is_renewal = 1 AND o.is_lost = 1))
                AND ((o.is_renewal = 1 AND o.arr_basis > 10000)
                        OR o.net_arr > 10000)
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_trx_over_10k_count,  -- ttm_trx_over_10k_count

          SUM (CASE
            WHEN (o.is_won = 1
                  OR (o.is_renewal = 1 AND o.is_lost = 1))
                AND ((o.is_renewal = 1 AND o.arr_basis > 50000)
                        OR o.net_arr > 50000)
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_trx_over_50k_count,  -- ttm_trx_over_50k_count

          SUM (CASE
            WHEN o.is_renewal = 1
            THEN o.calculated_deal_count
            ELSE 0 END )   AS last_12m_booked_renewal_deal_count,   -- ttm_renewal_deal_count

        SUM(CASE
            WHEN o.is_eligible_churn_contraction_flag = 1
                AND o.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Contract Reset','Contract Reset/Ramp Deal')
            THEN o.calculated_deal_count
            ELSE 0 END) AS last_12m_booked_churn_contraction_deal_count,  -- ttm_churn_contraction_deal_count

          -- deal path direct year
        SUM(CASE
            WHEN (o.deal_path != 'Channel' AND o.deal_path != 'Partner')
                AND o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END) AS last_12m_booked_direct_deal_count,  -- ttm_direct_deal_count

        -- deal path channel year
        SUM(CASE
            WHEN (o.deal_path = 'Channel' OR o.deal_path = 'Partner')
                AND o.is_won = 1
            THEN o.calculated_deal_count
            ELSE 0 END) AS last_12m_booked_channel_deal_count  -- ttm_channel_deal_count

    FROM sfdc_opportunity_xf AS o
    CROSS JOIN report_dates AS d
    WHERE o.close_date BETWEEN DATEADD(month, -12,DATE_TRUNC('month',d.report_month_date)) and DATE_TRUNC('month',d.report_month_date)
        AND o.booked_net_arr <> 0
    GROUP BY 1, 2

  -- total booked net arr in fy
  ), fy_net_arr AS (

    SELECT
      o.account_id,
      o.close_fiscal_year   AS report_fiscal_year,
      SUM(o.booked_net_arr) AS fy_booked_net_arr,
      SUM(CASE
            WHEN  o.sales_qualified_source != 'Web Direct Generated'
              THEN o.booked_net_arr
            ELSE 0
          END)          AS fy_booked_non_web_booked_net_arr,
      SUM(CASE
            WHEN o.sales_qualified_source = 'Web Direct Generated'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_web_direct_sourced_net_arr,
      SUM(CASE
            WHEN (o.sales_qualified_source = 'Partner Generated' OR o.sales_qualified_source = 'Partner Generated')
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_channel_sourced_net_arr,
      SUM(CASE
            WHEN o.sales_qualified_source = 'SDR Generated'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_sdr_sourced_net_arr,
      SUM(CASE
            WHEN o.sales_qualified_source = 'AE Generated'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_ae_sourced_net_arr,
      SUM(CASE
            WHEN o.is_eligible_churn_contraction_flag = 1
            THEN o.booked_churned_contraction_net_arr
            ELSE 0 END) AS fy_booked_churn_contraction_net_arr,

        -- First Order year
        SUM(CASE
            WHEN o.order_type_stamped = '1. New - First Order'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_fo_net_arr,

        -- New Connected year
        SUM(CASE
            WHEN o.order_type_stamped = '2. New - Connected'
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_new_connected_net_arr,

        -- Growth year
        SUM(CASE
            WHEN o.order_type_stamped NOT IN ('2. New - Connected','1. New - First Order')
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_growth_net_arr,

        SUM(o.calculated_deal_count)   AS fy_booked_deal_count,

        -- deal path direct year
        SUM(CASE
            WHEN (o.deal_path != 'Channel' AND o.deal_path != 'Partner')
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_direct_net_arr,

        -- deal path channel year
        SUM(CASE
            WHEN (o.deal_path = 'Channel' OR o.deal_path = 'Partner')
            THEN o.booked_net_arr
            ELSE 0 END) AS fy_booked_channel_net_arr,

         -- deal path direct year
        SUM(CASE
            WHEN (o.deal_path != 'Channel' AND o.deal_path != 'Partner')
            THEN o.calculated_deal_count
            ELSE 0 END) AS fy_booked_direct_deal_count,

        -- deal path channel year
        SUM(CASE
            WHEN (o.deal_path = 'Channel' OR o.deal_path = 'Partner')
            THEN o.calculated_deal_count
            ELSE 0 END) AS fy_booked_channel_deal_count

    FROM sfdc_opportunity_xf AS o
    WHERE o.booked_net_arr <> 0
    GROUP BY 1,2

  -- Total open pipeline at the same point in previous fiscal years (total open pipe)
  ), op_forward_one_year AS (

    SELECT
      h.account_id,
      h.snapshot_fiscal_year        AS report_fiscal_year,
      -- net arr pipeline
      SUM(h.net_arr)                AS total_open_pipe,
      SUM(CASE
              WHEN h.close_fiscal_year = h.snapshot_fiscal_year + 1
                  THEN h.net_arr
              ELSE 0
          END)                      AS nfy_open_pipeline,
      SUM(CASE
              WHEN h.close_fiscal_year = h.snapshot_fiscal_year
                  THEN h.net_arr
              ELSE 0
          END)                       AS fy_open_pipeline,

      -- deal count pipeline
      SUM(h.calculated_deal_count)   AS total_count_open_deals,
      SUM(CASE
              WHEN h.close_fiscal_year = h.snapshot_fiscal_year + 1
                  THEN h.net_arr
              ELSE 0
          END)                      AS nfy_count_open_deals,
      SUM(CASE
              WHEN h.close_fiscal_year = h.snapshot_fiscal_year
                  THEN h.net_arr
              ELSE 0
          END)                       AS fy_count_open_deals

    FROM sfdc_opportunity_snapshot_xf AS h
    WHERE h.close_date > h.snapshot_date
      AND h.forecast_category_name NOT IN  ('Omitted','Closed')
      AND h.order_type_stamped != '7. PS / Other'
      AND h.net_arr != 0
      AND h.is_eligible_open_pipeline_flag = 1
      GROUP BY 1,2

  -- Last 12 months pipe gen at same point of time in the year
  ), pg_last_12_months AS (

    SELECT
      h.account_id,
      h.snapshot_fiscal_year AS report_fiscal_year,
      SUM(h.net_arr)                 AS pg_last_12m_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'Web Direct Generated'
            THEN h.net_arr
            ELSE 0 END)              AS pg_last_12m_web_direct_sourced_net_arr,
      SUM(CASE
            WHEN (h.sales_qualified_source = 'Partner Generated' OR h.sales_qualified_source = 'Partner Generated')
            THEN h.net_arr
            ELSE 0 END)              AS pg_last_12m_channel_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'SDR Generated'
            THEN h.net_arr
            ELSE 0 END)              AS pg_last_12m_sdr_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'AE Generated'
            THEN h.net_arr
            ELSE 0 END)              AS pg_last_12m_ae_sourced_net_arr,

      SUM(CASE
            WHEN h.sales_qualified_source = 'Web Direct Generated'
            THEN h.calculated_deal_count
            ELSE 0 END)              AS pg_last_12m_web_direct_sourced_deal_count,
      SUM(CASE
            WHEN (h.sales_qualified_source = 'Partner Generated' OR h.sales_qualified_source = 'Partner Generated')
            THEN h.calculated_deal_count
            ELSE 0 END)              AS pg_last_12m_channel_sourced_deal_count,
      SUM(CASE
            WHEN h.sales_qualified_source = 'SDR Generated'
            THEN h.calculated_deal_count
            ELSE 0 END)              AS pg_last_12m_sdr_sourced_deal_count,
      SUM(CASE
            WHEN h.sales_qualified_source = 'AE Generated'
            THEN h.calculated_deal_count
            ELSE 0 END)              AS pg_last_12m_ae_sourced_deal_count

    FROM sfdc_opportunity_snapshot_xf AS h

    -- pipeline created within the last 12 months
    WHERE
        h.pipeline_created_date > dateadd(month,-12,h.snapshot_date)
      AND h.pipeline_created_date <= h.snapshot_date
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_created_pipeline_flag = 1
    GROUP BY 1,2

  -- Pipe generation at the same point in time in the fiscal year
  ), pg_ytd AS (

    SELECT
      h.account_id,
      h.net_arr_created_fiscal_year  AS report_fiscal_year,
      SUM(h.net_arr)                 AS pg_ytd_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'Web Direct Generated'
            THEN h.net_arr
            ELSE 0 END) AS pg_ytd_web_direct_sourced_net_arr,
      SUM(CASE
            WHEN (h.sales_qualified_source = 'Partner Generated' OR h.sales_qualified_source = 'Partner Generated')
            THEN h.net_arr
            ELSE 0 END) AS pg_ytd_channel_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'SDR Generated'
            THEN h.net_arr
            ELSE 0 END) AS pg_ytd_sdr_sourced_net_arr,
      SUM(CASE
            WHEN h.sales_qualified_source = 'AE Generated'
            THEN h.net_arr
            ELSE 0 END) AS pg_ytd_ae_sourced_net_arr
    FROM sfdc_opportunity_snapshot_xf AS h
      -- pipeline created within the fiscal year
    WHERE h.snapshot_fiscal_year = h.net_arr_created_fiscal_year
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_created_pipeline_flag = 1
      AND h.net_arr > 0
      GROUP BY 1,2

  -- ARR at the same point in time in Fiscal Year
  ), arr_at_same_month AS (

    SELECT
      mrr.dim_crm_account_id AS account_id,
      mrr_date.fiscal_year   AS report_fiscal_year,
  --    ultimate_parent_account_id,
      SUM(mrr.mrr)      AS mrr,
      SUM(mrr.arr)      AS arr,
      SUM(CASE
              WHEN sub.is_channel_arr_flag = 1
                  THEN mrr.arr
              ELSE 0
          END)          AS reseller_arr,
      SUM(CASE
              WHEN  sub.is_channel_arr_flag = 0
                  THEN mrr.arr
              ELSE 0
          END)          AS direct_arr,

      SUM(CASE
              WHEN  (mrr.product_tier_name LIKE '%Starter%'
                      OR mrr.product_tier_name LIKE '%Bronze%')
                  THEN mrr.arr
              ELSE 0
          END)          AS product_starter_arr,

      SUM(CASE
              WHEN  mrr.product_tier_name LIKE '%Premium%'
                  THEN mrr.arr
              ELSE 0
          END)          AS product_premium_arr,
      SUM(CASE
              WHEN  mrr.product_tier_name LIKE '%Ultimate%'
                  THEN mrr.arr
              ELSE 0
          END)          AS product_ultimate_arr,

      SUM(CASE
              WHEN  mrr.product_tier_name LIKE '%Self-Managed%'
                  THEN mrr.arr
              ELSE 0
          END)          AS delivery_self_managed_arr,
      SUM(CASE
              WHEN  mrr.product_tier_name LIKE '%SaaS%'
                  THEN mrr.arr
              ELSE 0
          END)          AS delivery_saas_arr

    FROM mart_arr AS mrr
    INNER JOIN date_details AS mrr_date
      ON mrr.arr_month = mrr_date.date_actual
    INNER JOIN dim_subscription AS sub
      ON sub.dim_subscription_id = mrr.dim_subscription_id
    WHERE mrr_date.month_actual =  (SELECT DISTINCT month_actual
                                      FROM date_details
                                      WHERE date_actual = DATE_TRUNC('month', DATEADD(month, -1, DATEADD(day, -2, CURRENT_DATE))))
    GROUP BY 1,2

), sao_last_12_month AS (

  SELECT
        h.sales_accepted_fiscal_year   AS report_fiscal_year,
        h.account_id,
        SUM(h.calculated_deal_count)    AS last_12m_sao_deal_count,
        SUM(h.net_arr)                  AS last_12m_sao_net_arr,
        SUM(h.booked_net_arr)           AS last_12m_sao_booked_net_arr

  FROM sfdc_opportunity_snapshot_xf AS h
    WHERE
        h.sales_accepted_date > dateadd(month,-12,h.snapshot_date)
      AND h.sales_accepted_date <= h.snapshot_date
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_sao_flag = 1
      AND h.is_renewal = 0
    GROUP BY 1,2

), sao_fy AS (

  SELECT
        h.sales_accepted_fiscal_year   AS report_fiscal_year,
        h.account_id,
        SUM(h.calculated_deal_count)    AS fy_sao_deal_count,
        SUM(h.net_arr)                  AS fy_sao_net_arr,
        SUM(h.booked_net_arr)           AS fy_sao_booked_net_arr

  FROM sfdc_opportunity_snapshot_xf AS h
    WHERE
       h.snapshot_fiscal_year = h.sales_accepted_fiscal_year
      AND h.sales_accepted_date <= h.snapshot_date
      AND h.order_type_stamped != '7. PS / Other'
      AND h.is_eligible_sao_flag = 1
      AND h.is_renewal = 0
    GROUP BY 1,2

), consolidated_accounts AS (

  SELECT
    ak.report_fiscal_year,
    raw_acc.account_id                      AS account_id,
    raw_acc.account_name                    AS account_name,
    raw_acc.upa_id,
    raw_acc.upa_name,
    raw_acc.is_key_account,
    raw_acc.abm_tier,
    raw_acc.upa_id                      AS ultimate_parent_account_id,
    u.name                              AS account_owner_name,
    raw_acc.account_owner_id,
    trim(u.employee_number)             AS account_owner_employee_number,
    upa_owner.name                      AS upa_owner_name,
    upa_owner.user_id                   AS upa_owner_id,
    upa_owner.title_category            AS upa_owner_title_category,
    trim(upa_owner.employee_number)     AS upa_owner_employee_number,

    -- for planning we need a local address for the target account
    raw_acc.account_billing_country_name,
    raw_acc.account_billing_country_code,
    raw_acc.account_billing_state_name,
    raw_acc.account_billing_state_code,
    raw_acc.account_billing_city,
    raw_acc.account_billing_postal_code,

    raw_acc.account_demographics_upa_territory,
    raw_acc.account_demographics_upa_state_code,
    raw_acc.account_demographics_upa_state_name,
    raw_acc.account_demographics_upa_country_code,
    raw_acc.account_demographics_upa_country_name,
    raw_acc.account_demographics_upa_city,
    raw_acc.account_demographics_upa_postal_code,
    raw_acc.account_demographics_upa_business_unit,
    raw_acc.account_demographics_upa_geo,
    raw_acc.account_demographics_upa_region,
    raw_acc.account_demographics_upa_area,
    raw_acc.account_demographics_upa_sales_segment,
    raw_acc.pubsec_type,
    --raw_acc.public_sector_account_flag,
    raw_acc.lam_arr,
    raw_acc.customer_score,

    -- substitute this by key segment
    u.business_unit                               AS account_user_business_unit,
    u.user_geo                                    AS account_user_geo,
    u.user_region                                 AS account_user_region,
    u.user_segment                                AS account_user_segment,
    u.user_area                                   AS account_user_area,
    u.role_name                                   AS account_owner_role,
    u.title_category                              AS account_owner_title_category,
    raw_acc.account_industry,
    raw_acc.account_demographics_territory,

    upa_owner.user_geo                            AS upa_user_geo,
    upa_owner.user_region                         AS upa_user_region,
    upa_owner.user_segment                        AS upa_user_segment,
    upa_owner.user_area                           AS upa_user_area,
    upa_owner.role_name                           AS upa_user_role,
    raw_acc.upa_industry                          AS upa_industry,

    -- NF: These fields are only useful to calculate LAM Dev Count which is already calculated
    raw_acc.potential_users,
    raw_acc.licenses,
    raw_acc.linkedin_developer,
    raw_acc.zi_developers,
    raw_acc.zi_revenue,
    raw_acc.employees,
    raw_acc.account_family_arr,
    raw_acc.calculated_developer_count,

    -- LAM Dev count calculated at the UPA level
    raw_acc.upa_lam_dev_count,
    raw_acc.account_health_score_color,
    raw_acc.account_health_number,
    --raw_acc.account_demographics_upa_max_family_employees,
    --raw_acc.upa_pubsec_type,

    -- atr for current fy
    COALESCE(fy_atr_base.fy_atr,0)           AS fy_atr,
    -- next fiscal year atr base reported at fy
    COALESCE(nfy_atr_base.nfy_atr,0)         AS nfy_atr,
    COALESCE(nfy_atr_base.nfy_q1_atr,0)      AS nfy_q1_atr,
    COALESCE(nfy_atr_base.nfy_q2_atr,0)      AS nfy_q2_atr,
    COALESCE(nfy_atr_base.nfy_q3_atr,0)      AS nfy_q3_atr,
    COALESCE(nfy_atr_base.nfy_q4_atr,0)      AS nfy_q4_atr,
    -- last 12 months ATR
    COALESCE(last_12m_atr_base.last_12m_atr,0)    AS last_12m_atr,

    -- arr by fy
    COALESCE(arr.arr,0)                           AS arr,

    COALESCE(arr.reseller_arr,0)                  AS arr_channel,
    COALESCE(arr.direct_arr,0)                    AS arr_direct,

    COALESCE(arr.product_starter_arr,0)           AS product_starter_arr,
    COALESCE(arr.product_premium_arr,0)           AS product_premium_arr,
    COALESCE(arr.product_ultimate_arr,0)          AS product_ultimate_arr,


    CASE
      WHEN COALESCE(arr.product_ultimate_arr,0) > COALESCE(arr.product_starter_arr,0) + COALESCE(arr.product_premium_arr,0)
          THEN 1
      ELSE 0
    END                                           AS is_ultimate_customer_flag,

    CASE
      WHEN COALESCE(arr.product_ultimate_arr,0) < COALESCE(arr.product_starter_arr,0) + COALESCE(arr.product_premium_arr,0)
          THEN 1
      ELSE 0
    END                                           AS is_premium_customer_flag,

    COALESCE(arr.delivery_self_managed_arr,0)     AS delivery_self_managed_arr,
    COALESCE(arr.delivery_saas_arr,0)             AS delivery_saas_arr,

    -- accounts counts
    CASE
      WHEN COALESCE(arr.arr,0) = 0
      THEN 1
      ELSE 0
    END                                           AS is_prospect_flag,

    CASE
      WHEN COALESCE(arr.arr,0) > 0
      THEN 1
      ELSE 0
    END                                           AS is_customer_flag,

    CASE
      WHEN COALESCE(arr.arr,0) > 5000
      THEN 1
      ELSE 0
    END                                           AS is_over_5k_customer_flag,
    CASE
      WHEN COALESCE(arr.arr,0) > 10000
      THEN 1
      ELSE 0
    END                                           AS is_over_10k_customer_flag,
    CASE
      WHEN COALESCE(arr.arr,0) > 50000
      THEN 1
      ELSE 0
    END                                           AS is_over_50k_customer_flag,

    CASE
      WHEN COALESCE(arr.arr,0) > 100000
      THEN 1
      ELSE 0
    END                                           AS is_over_100k_customer_flag,

    CASE
      WHEN COALESCE(arr.arr,0) > 500000
      THEN 1
      ELSE 0
    END                                           AS is_over_500k_customer_flag,

    -- rolling last 12 months booked net arr
    COALESCE(net_arr_last_12m.last_12m_booked_net_arr,0)                       AS last_12m_booked_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_non_web_net_arr,0)               AS last_12m_booked_non_web_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_web_direct_sourced_net_arr,0)    AS last_12m_booked_web_direct_sourced_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_channel_sourced_net_arr,0)       AS last_12m_booked_channel_sourced_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_sdr_sourced_net_arr,0)           AS last_12m_booked_sdr_sourced_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_ae_sourced_net_arr,0)            AS last_12m_booked_ae_sourced_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_churn_contraction_net_arr,0)     AS last_12m_booked_churn_contraction_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_fo_net_arr,0)                    AS last_12m_booked_fo_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_new_connected_net_arr,0)         AS last_12m_booked_new_connected_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_growth_net_arr,0)                AS last_12m_booked_growth_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_deal_count,0)                    AS last_12m_booked_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_direct_net_arr,0)                AS last_12m_booked_direct_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_channel_net_arr,0)               AS last_12m_booked_channel_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_channel_net_arr,0)  - COALESCE(net_arr_last_12m.last_12m_booked_channel_sourced_net_arr,0)   AS last_12m_booked_channel_co_sell_net_arr,
    COALESCE(net_arr_last_12m.last_12m_booked_direct_deal_count,0)             AS last_12m_booked_direct_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_channel_deal_count,0)            AS last_12m_booked_channel_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_churn_contraction_deal_count,0)  AS last_12m_booked_churn_contraction_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_renewal_deal_count,0)            AS last_12m_booked_renewal_deal_count,
    COALESCE(net_arr_last_12m.last_12m_booked_trx_count,0)                     AS last_12m_booked_trx_count,
    COALESCE(net_arr_last_12m.last_12m_booked_trx_over_5k_count,0)             AS last_12m_booked_trx_over_5k_count,
    COALESCE(net_arr_last_12m.last_12m_booked_trx_over_10k_count,0)            AS last_12m_booked_trx_over_10k_count,
    COALESCE(net_arr_last_12m.last_12m_booked_trx_over_50k_count,0)            AS last_12m_booked_trx_over_50k_count,

    -- fy booked net arr
    COALESCE(net_arr_fiscal.fy_booked_net_arr,0)                     AS fy_booked_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_web_direct_sourced_net_arr,0)  AS fy_booked_web_direct_sourced_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_channel_sourced_net_arr,0)     AS fy_booked_channel_sourced_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_sdr_sourced_net_arr,0)         AS fy_booked_sdr_sourced_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_ae_sourced_net_arr,0)          AS fy_booked_ae_sourced_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_churn_contraction_net_arr,0)   AS fy_booked_churn_contraction_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_fo_net_arr,0)                  AS fy_booked_fo_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_new_connected_net_arr,0)       AS fy_booked_new_connected_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_growth_net_arr,0)              AS fy_booked_growth_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_deal_count,0)                  AS fy_booked_deal_count,
    COALESCE(net_arr_fiscal.fy_booked_direct_net_arr,0)              AS fy_booked_direct_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_channel_net_arr,0)             AS fy_booked_channel_net_arr,
    COALESCE(net_arr_fiscal.fy_booked_direct_deal_count,0)           AS fy_booked_direct_deal_count,
    COALESCE(net_arr_fiscal.fy_booked_channel_deal_count,0)          AS fy_booked_channel_deal_count,

    -- open pipe forward looking
    COALESCE(op.total_open_pipe,0)                  AS total_open_pipe,
    COALESCE(op.total_count_open_deals,0)           AS total_count_open_deals_pipe,
    COALESCE(op.nfy_open_pipeline,0)                AS nfy_open_pipeline,
    COALESCE(op.fy_open_pipeline,0)                 AS fy_open_pipeline,
    COALESCE(op.nfy_count_open_deals,0)             AS nfy_count_open_deals,
    COALESCE(op.fy_count_open_deals,0)              AS fy_count_open_deals,

    CASE
      WHEN COALESCE(arr.arr,0) > 0
          AND COALESCE(op.total_open_pipe,0) > 0
              THEN 1
          ELSE 0
    END                                                       AS customer_has_open_pipe_flag,

    CASE
      WHEN COALESCE(arr.arr,0) = 0
          AND COALESCE(op.total_open_pipe,0) > 0
              THEN 1
          ELSE 0
    END                                                       AS prospect_has_open_pipe_flag,

    -- pipe generation
    COALESCE(pg.pg_ytd_net_arr,0)                             AS pg_ytd_net_arr,
    COALESCE(pg.pg_ytd_web_direct_sourced_net_arr,0)          AS pg_ytd_web_direct_sourced_net_arr,
    COALESCE(pg.pg_ytd_channel_sourced_net_arr,0)             AS pg_ytd_channel_sourced_net_arr,
    COALESCE(pg.pg_ytd_sdr_sourced_net_arr,0)                 AS pg_ytd_sdr_sourced_net_arr,
    COALESCE(pg.pg_ytd_ae_sourced_net_arr,0)                  AS pg_ytd_ae_sourced_net_arr,

    COALESCE(pg_ly.pg_last_12m_net_arr,0)                     AS pg_last_12m_net_arr,
    COALESCE(pg_ly.pg_last_12m_web_direct_sourced_net_arr,0)  AS pg_last_12m_web_direct_sourced_net_arr,
    COALESCE(pg_ly.pg_last_12m_channel_sourced_net_arr,0)     AS pg_last_12m_channel_sourced_net_arr,
    COALESCE(pg_ly.pg_last_12m_sdr_sourced_net_arr,0)         AS pg_last_12m_sdr_sourced_net_arr,
    COALESCE(pg_ly.pg_last_12m_ae_sourced_net_arr,0)          AS pg_last_12m_ae_sourced_net_arr,

    COALESCE(pg_last_12m_web_direct_sourced_deal_count,0)     AS pg_last_12m_web_direct_sourced_deal_count,
    COALESCE(pg_last_12m_channel_sourced_deal_count,0)        AS pg_last_12m_channel_sourced_deal_count,
    COALESCE(pg_last_12m_sdr_sourced_deal_count,0)            AS pg_last_12m_sdr_sourced_deal_count,
    COALESCE(pg_last_12m_ae_sourced_deal_count,0)             AS pg_last_12m_ae_sourced_deal_count,

    -- SAO metrics
    COALESCE(sao_last_12_month.last_12m_sao_deal_count,0)       AS last_12m_sao_deal_count,
    COALESCE(sao_last_12_month.last_12m_sao_net_arr,0)          AS last_12m_sao_net_arr,
    COALESCE(sao_last_12_month.last_12m_sao_booked_net_arr,0)   AS last_12m_sao_booked_net_arr,
    COALESCE(sao_fy.fy_sao_deal_count,0)                        AS fy_sao_deal_count,
    COALESCE(sao_fy.fy_sao_net_arr,0)                           AS fy_sao_net_arr,
    COALESCE(sao_fy.fy_sao_booked_net_arr,0)                    AS fy_sao_booked_net_arr,

     -- LAM Dev Count Category
    CASE
        WHEN raw_acc.upa_lam_dev_count < 100
            THEN '0. <100'
        WHEN raw_acc.upa_lam_dev_count >= 100
            AND raw_acc.upa_lam_dev_count < 250
            THEN '1. [100-250)'
        WHEN raw_acc.upa_lam_dev_count >= 250
            AND raw_acc.upa_lam_dev_count < 500
            THEN '2. [250-500)'
        WHEN raw_acc.upa_lam_dev_count >= 500
            AND raw_acc.upa_lam_dev_count < 1500
            THEN '3. [500-1500)'
        WHEN raw_acc.upa_lam_dev_count >= 1500
            AND raw_acc.upa_lam_dev_count < 2500
            THEN '4. [1500-2500)'
        WHEN raw_acc.upa_lam_dev_count >= 2500
            AND raw_acc.upa_lam_dev_count < 3500
            THEN '5. [2500-3500)'
        WHEN raw_acc.upa_lam_dev_count >= 3500
            AND raw_acc.upa_lam_dev_count < 5000
            THEN '6. [3500-5000)'
        WHEN raw_acc.upa_lam_dev_count >= 5000
            THEN '7. >5000'
    END AS lam_dev_count_bin_name,

    CASE
        WHEN raw_acc.upa_lam_dev_count < 100
            THEN 0
        WHEN raw_acc.upa_lam_dev_count >= 100
            AND raw_acc.upa_lam_dev_count < 250
            THEN 100
        WHEN raw_acc.upa_lam_dev_count >= 250
            AND raw_acc.upa_lam_dev_count < 500
            THEN 250
        WHEN raw_acc.upa_lam_dev_count >= 500
            AND raw_acc.upa_lam_dev_count < 1500
            THEN 500
        WHEN raw_acc.upa_lam_dev_count >= 1500
            AND raw_acc.upa_lam_dev_count < 2500
            THEN 1500
        WHEN raw_acc.upa_lam_dev_count >= 2500
            AND raw_acc.upa_lam_dev_count < 3500
            THEN 2500
        WHEN raw_acc.upa_lam_dev_count >= 3500
            AND raw_acc.upa_lam_dev_count < 5000
            THEN 3500
        WHEN raw_acc.upa_lam_dev_count >= 5000
            THEN 5000
    END AS lam_dev_count_bin_rank,

    -- Public Sector
    CASE
        WHEN lower(raw_acc.pubsec_type) ='row-pubsec'
            THEN 'Public'
        ELSE 'Private'
    END                     AS sector_type,
    CASE
        WHEN lower(raw_acc.pubsec_type) ='row-pubsec'
            THEN 1
        ELSE 0
    END                     AS is_public_sector_flag,

    ------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------
    -- Virtual UPA fields
    virtual_upa.virtual_upa_id,
    virtual_upa.virtual_upa_name,

    virtual_upa.virtual_upa_business_unit,
    virtual_upa.virtual_upa_owner_geo           AS virtual_upa_geo,
    virtual_upa.virtual_upa_owner_region        AS virtual_upa_region,
    virtual_upa.virtual_upa_owner_area          AS virtual_upa_area,
    virtual_upa.virtual_upa_segment             AS virtual_upa_segment,

    virtual_upa.virtual_upa_ad_business_unit,
    virtual_upa.virtual_upa_ad_geo,
    virtual_upa.virtual_upa_ad_region,
    virtual_upa.virtual_upa_ad_area,
    virtual_upa.virtual_upa_ad_segment,

    virtual_upa.virtual_upa_ad_country,
    virtual_upa.virtual_upa_ad_state_name,
    virtual_upa.virtual_upa_ad_state_code,
    virtual_upa.virtual_upa_ad_zip_code,

    virtual_upa.virtual_upa_industry,

    virtual_upa.virtual_upa_owner_name,
    virtual_upa.virtual_upa_owner_id,

    virtual_upa.virtual_upa_sub_business_unit,
    virtual_upa.virtual_upa_division,
    virtual_upa.virtual_upa_asm,

    virtual_upa.virtual_upa_country_name,
    virtual_upa.virtual_upa_state_name,
    virtual_upa.virtual_upa_state_code,
    virtual_upa.virtual_upa_zip_code,
    virtual_upa.upa_type                            AS virtual_upa_type


    ------------------------------------------------------------------------------------------
  FROM account_year_key AS ak
  INNER JOIN wk_sales_report_bob_virtual_hierarchy virtual_upa
      ON virtual_upa.account_id = ak.account_id
 -----
  INNER JOIN base_account AS raw_acc
      ON ak.account_id = raw_acc.account_id
-----
  LEFT JOIN sfdc_users_xf AS u
    ON raw_acc.account_owner_id = u.user_id
  LEFT JOIN sfdc_users_xf AS upa_owner
    ON raw_acc.upa_owner_id = upa_owner.user_id
-----
  LEFT JOIN fy_atr_base
    ON fy_atr_base.account_id = ak.account_id
    AND fy_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN last_12m_atr_base AS last_12m_atr_base
    ON last_12m_atr_base.account_id = ak.account_id
    AND last_12m_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN nfy_atr_base
    ON nfy_atr_base.account_id = ak.account_id
    AND nfy_atr_base.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN net_arr_last_12m
    ON net_arr_last_12m.account_id = ak.account_id
    AND net_arr_last_12m.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN op_forward_one_year AS op
    ON op.account_id = ak.account_id
    AND op.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN pg_ytd AS pg
    ON pg.account_id = ak.account_id
    AND pg.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN pg_last_12_months AS pg_ly
    ON pg_ly.account_id = ak.account_id
    AND pg_ly.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN arr_at_same_month AS arr
    ON arr.account_id = ak.account_id
    AND arr.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN fy_net_arr AS net_arr_fiscal
    ON net_arr_fiscal.account_id = ak.account_id
    AND net_arr_fiscal.report_fiscal_year = ak.report_fiscal_year
  -- SAOs
  LEFT JOIN sao_last_12_month
    ON sao_last_12_month.account_id = ak.account_id
    AND sao_last_12_month.report_fiscal_year = ak.report_fiscal_year
  LEFT JOIN sao_fy
    ON sao_fy.account_id = ak.account_id
    AND sao_fy.report_fiscal_year = ak.report_fiscal_year

    ------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------
    ------------------------------------------------------------------------------------------
    -- CONSOLIDATED UPA CODE

-- Adjust for hierarchies split between different geos
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

)
, final AS (

  SELECT

    acc.*,

    CASE
      WHEN upa.arr > 0 AND upa.arr < 5000
        THEN '1. 0-5k ARR'
      WHEN upa.arr >= 5000 AND upa.arr < 10000
        THEN '2. 0-10k ARR'
      WHEN upa.arr >= 10000 AND upa.arr < 50000
        THEN '3. 10k-50k ARR'
      WHEN upa.arr >= 50000 AND upa.arr < 100000
        THEN '4. 50K-100k ARR'
      WHEN upa.arr >= 100000 AND upa.arr < 500000
        THEN '5. 100k-500k ARR'
      WHEN upa.arr >= 500000 AND upa.arr < 1000000
        THEN '6. 500k-1M ARR'
      WHEN upa.arr >= 500000 AND upa.arr < 1000000
        THEN '7. >=1M ARR'
      ELSE 'n/a'
    END    AS account_family_arr_bin_name,

    COALESCE(upa.potential_users,0)                 AS upa_potential_users,
    COALESCE(upa.licenses,0)                        AS upa_licenses,
    COALESCE(upa.linkedin_developer,0)              AS upa_linkedin_developer,
    COALESCE(upa.zi_developers,0)                   AS upa_zi_developers,
    COALESCE(upa.zi_revenue,0)                      AS upa_zi_revenue,
    COALESCE(upa.employees,0)                       AS upa_employees,
    COALESCE(upa.count_of_customers,0)              AS upa_count_of_customers,
    COALESCE(upa.arr,0)                             AS upa_arr,

    CASE
        WHEN upa.upa_id = acc.account_id
            THEN 1
        ELSE 0
    END                                     AS is_upa_flag,

    upa.is_customer_flag                    AS hierarchy_is_customer_flag,

    -- FY25 GTM logic for segment migration
    CASE
        WHEN lower(acc.pubsec_type) ='row-pubsec'
            THEN 'ROW-PubSec'
         WHEN lower(acc.pubsec_type) ='us-pubsec'
            THEN 'PubSec'
        WHEN upa_employees > 4000
                THEN 'Large'
        WHEN upa_employees BETWEEN 101 AND 4000
                 OR upa.product_ultimate_arr > 1
                 OR upa.arr > 30000
                THEN 'Mid-Market'
        ELSE 'SMB'
    END                                     AS fy25_segment_calc

  FROM consolidated_accounts acc
    LEFT JOIN consolidated_upa upa
        ON upa.upa_id = acc.virtual_upa_id
        AND upa.report_fiscal_year = acc.report_fiscal_year

)

SELECT * 
FROM final