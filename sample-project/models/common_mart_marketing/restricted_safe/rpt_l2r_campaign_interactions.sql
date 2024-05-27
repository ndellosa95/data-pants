{{ simple_cte([
    ('person_base','mart_crm_person'),
    ('mart_crm_opportunity_stamped_hierarchy_hist', 'mart_crm_opportunity_stamped_hierarchy_hist'), 
    ('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('map_alternative_lead_demographics','map_alternative_lead_demographics'),
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_date','dim_date'),
    ('fct_campaign','fct_campaign'),
    ('dim_campaign', 'dim_campaign'),
    ('dim_crm_user', 'dim_crm_user'),
    ('sfdc_lead_history', 'sfdc_lead_history_source'),
    ('sfdc_contact_history', 'sfdc_contact_history_source')
]) }}

, person_base_with_tp AS (

    SELECT
  --IDs
      mart_crm_touchpoint.dim_crm_person_id,
      mart_crm_touchpoint.dim_crm_account_id,
      dim_crm_account.dim_parent_crm_account_id,
      person_base.dim_crm_user_id,
      person_base.crm_partner_id,
      person_base.partner_prospect_id,
      person_base.sfdc_record_id,
      mart_crm_touchpoint.dim_crm_touchpoint_id,
      mart_crm_touchpoint.dim_campaign_id,

  
  --Person Data
      person_base.email_hash,
      person_base.email_domain,
      person_base.was_converted_lead,
      person_base.email_domain_type,
      person_base.is_valuable_signup,
      person_base.status AS crm_person_status,
      person_base.lead_source,
      person_base.source_buckets,
      person_base.is_mql,
      person_base.is_inquiry,
      person_base.is_lead_source_trial,
      person_base.account_demographics_sales_segment_grouped,
      person_base.account_demographics_sales_segment,
      person_base.account_demographics_segment_region_grouped,
      person_base.zoominfo_company_employee_count,
      person_base.account_demographics_region,
      person_base.account_demographics_geo,
      person_base.account_demographics_area,
      person_base.account_demographics_upa_country,
      person_base.account_demographics_territory,
      person_base.person_first_country,
      person_base.partner_prospect_status,
      person_base.prospect_share_status,
      dim_crm_account.is_first_order_available,
      person_base.sales_segment_name AS person_sales_segment_name,
      person_base.sales_segment_grouped AS person_sales_segment_grouped,
      person_base.person_score,
      person_base.behavior_score,
      person_base.employee_bucket,
      person_base.leandata_matched_account_sales_Segment,
      person_base.sfdc_record_type,
      map_alternative_lead_demographics.employee_count_segment_custom,
      map_alternative_lead_demographics.employee_bucket_segment_custom,
      COALESCE(map_alternative_lead_demographics.employee_count_segment_custom, 
      map_alternative_lead_demographics.employee_bucket_segment_custom) AS inferred_employee_segment,
      map_alternative_lead_demographics.geo_custom,
      UPPER(map_alternative_lead_demographics.geo_custom) AS inferred_geo,
      CASE
          WHEN person_base.is_first_order_person = TRUE 
            THEN '1. New - First Order'
          ELSE '3. Growth'
      END AS person_order_type,
      last_utm_campaign,
      last_utm_content,
      lead_score_classification,
      is_defaulted_trial,

  --Person Dates
      person_base.true_inquiry_date,
      person_base.mql_date_latest_pt,
      person_base.legacy_mql_date_first_pt,
      person_base.mql_sfdc_date_pt,
      person_base.mql_date_first_pt,
      person_base.accepted_date,
      person_base.accepted_date_pt,
      person_base.qualifying_date,
      person_base.qualifying_date_pt,

  --Touchpoint Data
      'Person Touchpoint' AS touchpoint_type,
      mart_crm_touchpoint.bizible_touchpoint_date,
      mart_crm_touchpoint.bizible_touchpoint_position,
      mart_crm_touchpoint.bizible_touchpoint_source,
      mart_crm_touchpoint.bizible_touchpoint_type,
      mart_crm_touchpoint.bizible_ad_campaign_name,
      mart_crm_touchpoint.bizible_form_url,
      mart_crm_touchpoint.bizible_landing_page,
      mart_crm_touchpoint.bizible_form_url_raw,
      mart_crm_touchpoint.bizible_landing_page_raw,
      -- UTM Parameters 
      mart_crm_touchpoint.utm_campaign,
      mart_crm_touchpoint.utm_medium,
      mart_crm_touchpoint.utm_source,
      mart_crm_touchpoint.utm_budget,
      mart_crm_touchpoint.utm_content,
      mart_crm_touchpoint.utm_allptnr,
      mart_crm_touchpoint.utm_partnerid,
      mart_crm_touchpoint.integrated_budget_holder,
      mart_crm_touchpoint.touchpoint_offer_type,
      mart_crm_touchpoint.touchpoint_offer_type_grouped,
      -- Touchpoint Data Cont.
      mart_crm_touchpoint.bizible_marketing_channel,
      mart_crm_touchpoint.bizible_marketing_channel_path,
      mart_crm_touchpoint.bizible_medium,
      mart_crm_touchpoint.bizible_referrer_page,
      mart_crm_touchpoint.bizible_referrer_page_raw,
      mart_crm_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_touchpoint.bizible_salesforce_campaign,
      mart_crm_touchpoint.bizible_ad_group_name,
      mart_crm_touchpoint.campaign_rep_role_name,
      mart_crm_touchpoint.touchpoint_segment,
      mart_crm_touchpoint.gtm_motion,
      mart_crm_touchpoint.pipe_name,
      mart_crm_touchpoint.is_dg_influenced,
      mart_crm_touchpoint.is_dg_sourced,
      mart_crm_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_touchpoint.is_fmm_influenced,
      mart_crm_touchpoint.is_fmm_sourced,
      mart_crm_touchpoint.devrel_campaign_type,
      mart_crm_touchpoint.devrel_campaign_description,
      mart_crm_touchpoint.devrel_campaign_influence_type,
      mart_crm_touchpoint.bizible_count_lead_creation_touch AS new_lead_created_sum,
      mart_crm_touchpoint.count_true_inquiry AS count_true_inquiry,
      mart_crm_touchpoint.count_inquiry AS inquiry_sum, 
      mart_crm_touchpoint.pre_mql_weight AS mql_sum,
      mart_crm_touchpoint.count_accepted AS accepted_sum,
      mart_crm_touchpoint.count_net_new_mql AS new_mql_sum,
      mart_crm_touchpoint.count_net_new_accepted AS new_accepted_sum    
    FROM mart_crm_touchpoint
    LEFT JOIN person_base 
      ON mart_crm_touchpoint.dim_crm_person_id = person_base.dim_crm_person_id
       AND mart_crm_touchpoint.email_hash = person_base.email_hash
    LEFT JOIN map_alternative_lead_demographics
      ON mart_crm_touchpoint.dim_crm_person_id=map_alternative_lead_demographics.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON person_base.dim_crm_account_id=dim_crm_account.dim_crm_account_id

  ), opp_base_with_batp AS (
    
    SELECT
    --IDs
      opp.dim_crm_opportunity_id,
      opp.dim_crm_account_id,
      dim_crm_account.dim_parent_crm_account_id,
      mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
      opp.dim_crm_user_id AS opp_dim_crm_user_id,
      opp.duplicate_opportunity_id,
      opp.merged_crm_opportunity_id,
      opp.ssp_id,
      opp.primary_campaign_source_id AS opp_primary_campaign_source_id,
      opp.owner_id AS opp_owner_id,
      mart_crm_attribution_touchpoint.dim_campaign_id,
      partner_account.crm_account_name AS partner_account_name,
      partner_account.dim_crm_account_id AS opp_partner_dim_crm_account_id,

	--Opp Dates
      opp.created_date AS opp_created_date,
      opp.sales_accepted_date,
      opp.close_date,
      opp.subscription_start_date,
      opp.subscription_end_date,
      opp.pipeline_created_date,

	--Opp Flags
      opp.is_sao,
      opp.is_won,
      opp.is_net_arr_closed_deal,
      opp.is_jihu_account,
      opp.is_closed,
      opp.is_edu_oss,
      opp.is_ps_opp,
      opp.is_win_rate_calc,
      opp.is_net_arr_pipeline_created,
      opp.is_new_logo_first_order,
      opp.is_closed_won,
      opp.is_web_portal_purchase,
      opp.is_lost,
      opp.is_open,
      opp.is_renewal,
      opp.is_duplicate,
      opp.is_refund,
      opp.is_deleted,
      opp.is_excluded_from_pipeline_created,
      opp.is_contract_reset,
      opp.is_booked_net_arr,
      opp.is_downgrade,
      opp.critical_deal_flag,
      opp.is_public_sector_opp,
      opp.is_registration_from_portal,

    --Opp Data
      opp.new_logo_count,
      opp.net_arr,
      opp.amount,
      opp.invoice_number,
      opp.order_type AS opp_order_type,
      opp.sales_qualified_source_name,
      opp.deal_path_name,
      opp.sales_type,
      opp.crm_opp_owner_geo_stamped,
      opp.crm_opp_owner_sales_segment_stamped,
      opp.crm_opp_owner_region_stamped,
      opp.crm_opp_owner_area_stamped,
      opp.parent_crm_account_upa_country,
      opp.parent_crm_account_territory,
      opp.opportunity_name,
      opp.crm_account_name,
      opp.parent_crm_account_name,
      opp.stage_name,
      opp.closed_buckets,
      opp.opportunity_category,
      opp.source_buckets AS opp_source_buckets,
      opp.opportunity_sales_development_representative,
      opp.opportunity_business_development_representative,
      opp.opportunity_development_representative,
      opp.sdr_or_bdr,
      opp.sdr_pipeline_contribution,
      opp.sales_path,
      opp.opportunity_deal_size,
      opp.net_new_source_categories AS opp_net_new_source_categories,
      opp.deal_path_engagement,
      opp.opportunity_owner,
      opp.order_type_grouped AS opp_order_type_grouped,
      opp.sales_qualified_source_grouped,
      opp.crm_account_gtm_strategy,
      opp.crm_account_focus_account,
      opp.crm_opp_owner_sales_segment_stamped_grouped,
      opp.crm_opp_owner_sales_segment_region_stamped_grouped,
      opp.lead_source AS opp_lead_source,
      opp.calculated_deal_count,
      opp.days_in_stage,
      opp.record_type_name,
      CASE
        WHEN opp.dr_deal_id IS NOT null
          THEN TRUE
        ELSE FALSE
      END AS is_created_through_deal_registration,
      opp.resale_partner_name,

    --Person Data
      person_base.dim_crm_person_id,
      person_base.dim_crm_user_id,
      person_base.crm_partner_id,
      person_base.sfdc_record_id,
      person_base.email_hash,
      person_base.email_domain,
      person_base.was_converted_lead,
      person_base.email_domain_type,
      person_base.is_valuable_signup,
      person_base.status AS crm_person_status,
      person_base.lead_source,
      person_base.source_buckets,
      person_base.is_mql,
      person_base.is_inquiry,
      person_base.is_lead_source_trial,
      person_base.account_demographics_sales_segment_grouped,
      person_base.account_demographics_sales_segment,
      person_base.account_demographics_segment_region_grouped,
      person_base.zoominfo_company_employee_count,
      person_base.account_demographics_region,
      person_base.account_demographics_geo,
      person_base.account_demographics_area,
      person_base.account_demographics_upa_country,
      person_base.account_demographics_territory,
      person_base.person_first_country,
      dim_crm_account.is_first_order_available,
      person_base.sales_segment_name AS person_sales_segment_name,
      person_base.sales_segment_grouped AS person_sales_segment_grouped,
      person_base.person_score,
      person_base.behavior_score,
      person_base.employee_bucket,
      person_base.leandata_matched_account_sales_Segment,
      person_base.sfdc_record_type,
      map_alternative_lead_demographics.employee_count_segment_custom,
      map_alternative_lead_demographics.employee_bucket_segment_custom,
      COALESCE(map_alternative_lead_demographics.employee_count_segment_custom, 
      map_alternative_lead_demographics.employee_bucket_segment_custom) AS inferred_employee_segment,
      map_alternative_lead_demographics.geo_custom,
      UPPER(map_alternative_lead_demographics.geo_custom) AS inferred_geo,
      CASE
          WHEN person_base.is_first_order_person = TRUE 
            THEN '1. New - First Order'
          ELSE '3. Growth'
      END AS person_order_type,
      last_utm_campaign,
      last_utm_content,
      person_base.prospect_share_status,
      person_base.partner_prospect_status,
      person_base.lead_score_classification,
      person_base.is_defaulted_trial,

  --Person Dates
      person_base.true_inquiry_date,
      person_base.mql_date_latest_pt,
      person_base.legacy_mql_date_first_pt,
      person_base.mql_sfdc_date_pt,
      person_base.mql_date_first_pt,
      person_base.accepted_date,
      person_base.accepted_date_pt,
      person_base.qualifying_date,
      person_base.qualifying_date_pt,
    
    -- Touchpoint Data
      'Attribution Touchpoint' AS touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_touchpoint_date,
      mart_crm_attribution_touchpoint.bizible_touchpoint_position,
      mart_crm_attribution_touchpoint.bizible_touchpoint_source,
      mart_crm_attribution_touchpoint.bizible_touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
      mart_crm_attribution_touchpoint.bizible_ad_group_name,
      mart_crm_attribution_touchpoint.bizible_form_url,
      mart_crm_attribution_touchpoint.bizible_landing_page,
      mart_crm_attribution_touchpoint.bizible_form_url_raw,
      mart_crm_attribution_touchpoint.bizible_landing_page_raw,
      mart_crm_attribution_touchpoint.touchpoint_sales_stage AS opp_touchpoint_sales_stage,
    -- UTM Parameters 
      mart_crm_attribution_touchpoint.utm_campaign,
      mart_crm_attribution_touchpoint.utm_medium,
      mart_crm_attribution_touchpoint.utm_source,
      mart_crm_attribution_touchpoint.utm_budget,
      mart_crm_attribution_touchpoint.utm_content,
      mart_crm_attribution_touchpoint.utm_allptnr,
      mart_crm_attribution_touchpoint.utm_partnerid,
      mart_crm_attribution_touchpoint.integrated_budget_holder,
      mart_crm_attribution_touchpoint.touchpoint_offer_type,
      mart_crm_attribution_touchpoint.touchpoint_offer_type_grouped,
      -- Touchpoint Data Cont.
      mart_crm_attribution_touchpoint.bizible_marketing_channel,
      mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
      mart_crm_attribution_touchpoint.bizible_medium,
      mart_crm_attribution_touchpoint.bizible_referrer_page,
      mart_crm_attribution_touchpoint.bizible_referrer_page_raw,
      mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_attribution_touchpoint.bizible_salesforce_campaign,
	    mart_crm_attribution_touchpoint.campaign_rep_role_name,
      mart_crm_attribution_touchpoint.touchpoint_segment,
      mart_crm_attribution_touchpoint.gtm_motion,
      mart_crm_attribution_touchpoint.pipe_name,
      mart_crm_attribution_touchpoint.is_dg_influenced,
      mart_crm_attribution_touchpoint.is_dg_sourced,
      mart_crm_attribution_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_attribution_touchpoint.bizible_count_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_first_touch,
      mart_crm_attribution_touchpoint.is_fmm_influenced,
      mart_crm_attribution_touchpoint.is_fmm_sourced,
      mart_crm_attribution_touchpoint.devrel_campaign_type,
      mart_crm_attribution_touchpoint.devrel_campaign_description,
      mart_crm_attribution_touchpoint.devrel_campaign_influence_type,
      CASE
          WHEN mart_crm_attribution_touchpoint.dim_crm_touchpoint_id IS NOT null THEN opp.dim_crm_opportunity_id
          ELSE null
      END AS influenced_opportunity_id,
      SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) AS custom_opp_created,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS custom_sao,
      CASE 
        WHEN opp.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS pipeline_custom_net_arr,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS won_custom,
      CASE 
        WHEN opp.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS won_custom_net_arr

    FROM mart_crm_attribution_touchpoint
    LEFT JOIN person_base
      ON mart_crm_attribution_touchpoint.dim_crm_person_id = person_base.dim_crm_person_id
    LEFT JOIN mart_crm_opportunity_stamped_hierarchy_hist opp
      ON mart_crm_attribution_touchpoint.dim_crm_opportunity_id=opp.dim_crm_opportunity_id
    LEFT JOIN map_alternative_lead_demographics
      ON person_base.dim_crm_person_id=map_alternative_lead_demographics.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON opp.dim_crm_account_id=dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_account partner_account
      ON opp.partner_account=partner_account.dim_crm_account_id
  {{dbt_utils.group_by(n=186)}}
    
), cohort_base_combined AS (
  
    SELECT
	--IDs
      dim_crm_person_id,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      dim_crm_user_id,
      crm_partner_id,
      partner_prospect_id,
      sfdc_record_id,
      dim_crm_touchpoint_id,
      dim_campaign_id,
      null AS dim_crm_opportunity_id,
      null AS opp_dim_crm_user_id,
      null AS duplicate_opportunity_id,
      null AS merged_crm_opportunity_id,
      null AS ssp_id,
      null AS opp_primary_campaign_source_id,
      null AS opp_owner_id,
      null AS partner_account_name,

  --Person Data
      email_hash,
      email_domain,
      was_converted_lead,
      email_domain_type,
      is_valuable_signup,
      crm_person_status,
      lead_source,
      source_buckets,
      is_mql,
      is_inquiry,
      is_lead_source_trial,
      account_demographics_sales_segment_grouped,
      account_demographics_sales_segment,
      account_demographics_segment_region_grouped,
      zoominfo_company_employee_count,
      account_demographics_region,
      account_demographics_geo,
      account_demographics_area,
      account_demographics_upa_country,
      account_demographics_territory,
      person_first_country,
      is_first_order_available,
      person_order_type,
      person_sales_segment_name,
      person_sales_segment_grouped,
      person_score,
      behavior_score,
      employee_bucket,
      leandata_matched_account_sales_Segment,
      sfdc_record_type,
      employee_count_segment_custom,
      employee_bucket_segment_custom,
      inferred_employee_segment,
      geo_custom,
      inferred_geo,
      last_utm_campaign,
      last_utm_content,
      prospect_share_status,
      partner_prospect_status,
      lead_score_classification,
      is_defaulted_trial,

  --Person Dates
      true_inquiry_date,
      mql_date_latest_pt,
      legacy_mql_date_first_pt,
      mql_sfdc_date_pt,
      mql_date_first_pt,
      accepted_date,
      accepted_date_pt,
      qualifying_date,
      qualifying_date_pt,
  
  --Opp Dates
      null AS opp_created_date,
      null AS sales_accepted_date,
      null AS close_date,
      null AS subscription_start_date,
      null AS subscription_end_date,
      null AS pipeline_created_date,

	--Opp Flags
      null AS is_sao,
      null AS is_won,
      null AS is_net_arr_closed_deal,
      null AS is_jihu_account,
      null AS is_closed,
      null AS is_edu_oss,
      null AS is_ps_opp,
      null AS is_win_rate_calc,
      null AS is_net_arr_pipeline_created,
      null AS is_new_logo_first_order,
      null AS is_closed_won,
      null AS is_web_portal_purchase,
      null AS is_lost,
      null AS is_open,
      null AS is_renewal,
      null AS is_duplicate,
      null AS is_refund,
      null AS is_deleted,
      null AS is_excluded_from_pipeline_created,
      null AS is_contract_reset,
      null AS is_booked_net_arr,
      null AS is_downgrade,
      null AS critical_deal_flag,
      null AS is_public_sector_opp,
      null AS is_registration_from_portal,
      null AS is_created_through_deal_registration,

    --Opp Data
      null AS new_logo_count,
      null AS net_arr,
      null AS amount,
      null AS invoice_number,
      null AS opp_order_type,
      null AS sales_qualified_source_name,
      null AS deal_path_name,
      null AS sales_type,
      null AS crm_opp_owner_geo_stamped,
      null AS crm_opp_owner_sales_segment_stamped,
      null AS crm_opp_owner_region_stamped,
      null AS crm_opp_owner_area_stamped,
      null AS parent_crm_account_upa_country,
      null AS parent_crm_account_territory,
      null AS opportunity_name,
      null AS crm_account_name,
      null AS parent_crm_account_name,
      null AS stage_name,
      null AS closed_buckets,
      null AS opportunity_category,
      null AS opp_source_buckets,
      null AS opportunity_sales_development_representative,
      null AS opportunity_business_development_representative,
      null AS opportunity_development_representative,
      null AS sdr_or_bdr,
      null AS sdr_pipeline_contribution,
      null AS sales_path,
      null AS opportunity_deal_size,
      null AS opp_net_new_source_categories,
      null AS deal_path_engagement,
      null AS opportunity_owner,
      null AS sales_qualified_source_grouped,
      null AS crm_account_gtm_strategy,
      null AS crm_account_focus_account,
      null AS crm_opp_owner_sales_segment_stamped_grouped,
      null AS crm_opp_owner_sales_segment_region_stamped_grouped,
      null AS opp_lead_source,
      null AS calculated_deal_count,
      null AS days_in_stage,
      null AS record_type_name,
      null as resale_partner_name,
  
  --Touchpoint Data
      touchpoint_type,
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_landing_page,
      bizible_form_url_raw,
      bizible_landing_page_raw,
      NULL AS opp_touchpoint_sales_stage,
      -- UTM Parameters 
      utm_campaign,
      utm_medium,
      utm_source,
      utm_budget,
      utm_content,
      utm_allptnr,
      utm_partnerid,
      integrated_budget_holder,
      touchpoint_offer_type,
      touchpoint_offer_type_grouped,
      -- Touchpoint Data Cont.
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_integrated_campaign_grouping,
      bizible_salesforce_campaign,
	    campaign_rep_role_name,
      touchpoint_segment,
      gtm_motion,
      pipe_name,
      is_dg_influenced,
      is_dg_sourced,
      bizible_count_lead_creation_touch,
      is_fmm_influenced,
      is_fmm_sourced,
      devrel_campaign_type,
      devrel_campaign_description,
      devrel_campaign_influence_type,
      new_lead_created_sum,
      count_true_inquiry,
      inquiry_sum, 
      mql_sum,
      accepted_sum,
      new_mql_sum,
      new_accepted_sum,
      null AS bizible_count_custom_model,
      null AS bizible_weight_custom_model,
      null AS bizible_weight_first_touch,
      null AS influenced_opportunity_id,
      0 AS custom_sao,
      0 AS pipeline_custom_net_arr,
      0 AS won_custom,
      0 AS won_custom_net_arr
  FROM person_base_with_tp
  
  UNION ALL
  
    SELECT
    --IDs
      dim_crm_person_id,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      dim_crm_user_id,
      crm_partner_id,
      null AS partner_prospect_id,
      sfdc_record_id,
      dim_crm_touchpoint_id,
      dim_campaign_id,
      dim_crm_opportunity_id,
      opp_dim_crm_user_id,
      duplicate_opportunity_id,
      merged_crm_opportunity_id,
      ssp_id,
      opp_primary_campaign_source_id,
      opp_owner_id,
      partner_account_name,

    --Person Data
      email_hash,
      email_domain,
      was_converted_lead,
      email_domain_type,
      is_valuable_signup,
      crm_person_status,
      lead_source,
      source_buckets,
      is_mql,
      is_inquiry,
      is_lead_source_trial,
      account_demographics_sales_segment_grouped,
      account_demographics_sales_segment,
      account_demographics_segment_region_grouped,
      zoominfo_company_employee_count,
      account_demographics_region,
      account_demographics_geo,
      account_demographics_area,
      account_demographics_upa_country,
      account_demographics_territory,
      person_first_country,
      is_first_order_available,
      person_order_type,
      person_sales_segment_name,
      person_sales_segment_grouped,
      person_score,
      behavior_score,
      employee_bucket,
      leandata_matched_account_sales_Segment,
      sfdc_record_type,
      employee_count_segment_custom,
      employee_bucket_segment_custom,
      inferred_employee_segment,
      geo_custom,
      inferred_geo,
      last_utm_campaign,
      last_utm_content,
      prospect_share_status,
      partner_prospect_status,
      lead_score_classification,
      is_defaulted_trial,
    
    --Person Dates
      true_inquiry_date,
      mql_date_latest_pt,
      legacy_mql_date_first_pt,
      mql_sfdc_date_pt,
      mql_date_first_pt,
      accepted_date,
      accepted_date_pt,
      qualifying_date,
      qualifying_date_pt,

    --Opp Dates
      opp_created_date,
      sales_accepted_date,
      close_date,
      subscription_start_date,
      subscription_end_date,
      pipeline_created_date,

    --Opp Flags
      is_sao,
      is_won,
      is_net_arr_closed_deal,
      is_jihu_account,
      is_closed,
      is_edu_oss,
      is_ps_opp,
      is_win_rate_calc,
      is_net_arr_pipeline_created,
      is_new_logo_first_order,
      is_closed_won,
      is_web_portal_purchase,
      is_lost,
      is_open,
      is_renewal,
      is_duplicate,
      is_refund,
      is_deleted,
      is_excluded_from_pipeline_created,
      is_contract_reset,
      is_booked_net_arr,
      is_downgrade,
      critical_deal_flag,
      is_public_sector_opp,
      is_registration_from_portal,
      is_created_through_deal_registration,

      --Opp Data
      new_logo_count,
      net_arr,
      amount,
      invoice_number,
      opp_order_type,
      sales_qualified_source_name,
      deal_path_name,
      sales_type,
      crm_opp_owner_geo_stamped,
      crm_opp_owner_sales_segment_stamped,
      crm_opp_owner_region_stamped,
      crm_opp_owner_area_stamped,
      parent_crm_account_upa_country,
      parent_crm_account_territory,
      opportunity_name,
      crm_account_name,
      parent_crm_account_name,
      stage_name,
      closed_buckets,
      opportunity_category,
      opp_source_buckets,
      opportunity_sales_development_representative,
      opportunity_business_development_representative,
      opportunity_development_representative,
      sdr_or_bdr,
      sdr_pipeline_contribution,
      sales_path,
      opportunity_deal_size,
      opp_net_new_source_categories,
      deal_path_engagement,
      opportunity_owner,
      sales_qualified_source_grouped,
      crm_account_gtm_strategy,
      crm_account_focus_account,
      crm_opp_owner_sales_segment_stamped_grouped,
      crm_opp_owner_sales_segment_region_stamped_grouped,
      opp_lead_source,
      calculated_deal_count,
      days_in_stage,
      record_type_name,
      resale_partner_name,
    
      --Touchpoint Data
      touchpoint_type,
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_landing_page,
      bizible_form_url_raw,
      bizible_landing_page_raw,
      opp_touchpoint_sales_stage,
      -- UTM Parameters 
      utm_campaign,
      utm_medium,
      utm_source,
      utm_budget,
      utm_content,
      utm_allptnr,
      utm_partnerid,
      integrated_budget_holder,
      touchpoint_offer_type,
      touchpoint_offer_type_grouped,
      -- Touchpoint Data Cont.
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_integrated_campaign_grouping,
      bizible_salesforce_campaign,
      campaign_rep_role_name,
      touchpoint_segment,
      gtm_motion,
      pipe_name,
      is_dg_influenced,
      is_dg_sourced,
      bizible_count_lead_creation_touch,
      is_fmm_influenced,
      is_fmm_sourced,
      devrel_campaign_type,
      devrel_campaign_description,
      devrel_campaign_influence_type,
      0 AS new_lead_created_sum,
      0 AS count_true_inquiry,
      0 AS inquiry_sum, 
      0 AS mql_sum,
      0 AS accepted_sum,
      0 AS new_mql_sum,
      0 AS new_accepted_sum,
      bizible_count_custom_model,
      bizible_weight_custom_model,
      bizible_weight_first_touch,
      influenced_opportunity_id,
      custom_sao,
      pipeline_custom_net_arr,
      won_custom,
      won_custom_net_arr
    FROM opp_base_with_batp

), person_history_base AS (
    SELECT
      sfdc_lead_history.lead_id AS sfdc_record_id,
      cohort_base_combined.dim_crm_touchpoint_id,
      sfdc_lead_history.field_modified_at,
      cohort_base_combined.bizible_touchpoint_date,
      datediff(DAY, 
        cohort_base_combined.bizible_touchpoint_date,
        sfdc_lead_history.field_modified_at
      ) AS date_difference,
      sfdc_lead_history.old_value_string,
      sfdc_lead_history.new_value_string,
      'Lead' AS record_type
    FROM sfdc_lead_history
      INNER JOIN cohort_base_combined
        ON sfdc_lead_history.lead_id = cohort_base_combined.sfdc_record_id 
    WHERE 
    lead_field = 'status' AND field_modified_at >= bizible_touchpoint_date
    
    UNION ALL 

    SELECT
      sfdc_contact_history.contact_id AS sfdc_record_id,
      cohort_base_combined.dim_crm_touchpoint_id,
      sfdc_contact_history.field_modified_at,
      cohort_base_combined.bizible_touchpoint_date,
      datediff(DAY, 
        cohort_base_combined.bizible_touchpoint_date,
        sfdc_contact_history.field_modified_at
      ) AS date_difference,
      sfdc_contact_history.old_value_string,
      sfdc_contact_history.new_value_string,
      'Contact' as record_type
    FROM sfdc_contact_history
      INNER JOIN cohort_base_combined
        ON sfdc_contact_history.contact_id = cohort_base_combined.sfdc_record_id 
    WHERE contact_field = 'contact_status__c' AND field_modified_at >= bizible_touchpoint_date

), person_history_final as (
  SELECT
  person_history_base.*,
  person_history_base.old_value_string || ' -> ' || person_history_base.new_value_string as person_status_change,
  CASE WHEN person_status_change IS NULL THEN
    '[No Update]' 
    ELSE person_status_change
  END AS person_status_update,
  row_number() OVER (PARTITION BY sfdc_record_id, dim_crm_touchpoint_id ORDER BY field_modified_at ASC) AS history_update_rank
  FROM
  person_history_base
  QUALIFY history_update_rank = 1

), today AS (

  SELECT DISTINCT
    fiscal_year               AS current_fiscal_year,
    first_day_of_fiscal_year  AS current_fiscal_year_date
  FROM dim_date
  WHERE date_actual = CURRENT_DATE

), intermediate AS (
  
    SELECT 
      cohort_base_combined.*,
      PARSE_URL(bizible_form_url_raw):parameters:utm_asset_type::VARCHAR    AS bizible_form_page_utm_asset_type,
      PARSE_URL(bizible_landing_page_raw):parameters:utm_asset_type::VARCHAR AS bizible_landing_page_utm_asset_type,
      COALESCE(bizible_landing_page_utm_asset_type, bizible_form_page_utm_asset_type) AS utm_asset_type,
      
      -- campaign
      fct_campaign.start_date  AS campaign_start_date,
      fct_campaign.region      AS sfdc_campaign_region,
      fct_campaign.sub_region  AS sfdc_campaign_sub_region,
      dim_campaign.type        AS sfdc_campaign_type,
      fct_campaign.budgeted_cost,
      fct_campaign.actual_cost,
      dim_campaign.is_a_channel_partner_involved,
      dim_campaign.is_an_alliance_partner_involved,
      dim_campaign.channel_partner_name,
      dim_campaign.alliance_partner_name,
      campaign_owner.user_name          AS campaign_owner,
      campaign_owner_manager.user_name  AS campaign_owner_manager,
      CASE  
        WHEN dim_campaign.will_there_be_mdf_funding = 'Yes'
          THEN TRUE
          ELSE FALSE
      END AS is_mdf_campaign,

      -- Account
      dim_crm_account.abm_tier,
      dim_crm_account.health_number,
      dim_crm_account.crm_account_type,

     -- Opportunity Report Fields
     CASE 
        WHEN cohort_base_combined.close_date < today.current_fiscal_year_date
          THEN account_owner.crm_user_business_unit
        ELSE opportunity_owner.crm_user_business_unit
    END                                                       AS report_opportunity_user_business_unit,
    CASE 
        WHEN cohort_base_combined.close_date < today.current_fiscal_year_date
          THEN account_owner.crm_user_sub_business_unit
        ELSE opportunity_owner.crm_user_sub_business_unit
    END                                                       AS report_opportunity_user_sub_business_unit,
    CASE 
        WHEN account_owner.is_hybrid_user = 1 
            THEN dim_crm_account.parent_crm_account_area
        WHEN cohort_base_combined.close_date < today.current_fiscal_year_date
          THEN account_owner.asm
        WHEN UPPER(opportunity_owner.crm_user_area) = 'ALL'
           THEN dim_crm_account.parent_crm_account_area         
        ELSE opportunity_owner.asm
    END                                                       AS report_opportunity_user_asm,

      -- user
      user.user_name        AS record_owner_name,
      user.manager_name     AS record_owner_manager,
      user.title            AS record_owner_title,
      user.department       AS record_owner_department,
      user.team             AS record_owner_team,
      manager.manager_name  AS record_owner_sales_dev_leader,
      
      CASE
        WHEN  record_owner_title LIKE '%Sales Development%' 
          OR record_owner_title  LIKE '%Business Development%' 
        THEN TRUE
        ELSE FALSE
      END AS is_sales_dev_owned_record,

      person_history_final.person_status_update,
      person_history_final.person_status_change,

     --inquiry_date fields
      inquiry_date.fiscal_year                     AS inquiry_date_range_year,
      inquiry_date.fiscal_quarter_name_fy          AS inquiry_date_range_quarter,
      DATE_TRUNC(month, inquiry_date.date_actual)  AS inquiry_date_range_month,
      inquiry_date.first_day_of_week               AS inquiry_date_range_week,
      inquiry_date.date_id                         AS inquiry_date_range_id,
    
    --mql_date fields
      mql_date.fiscal_year                         AS mql_date_range_year,
      mql_date.fiscal_quarter_name_fy              AS mql_date_range_quarter,
      DATE_TRUNC(month, mql_date.date_actual)      AS mql_date_range_month,
      mql_date.first_day_of_week                   AS mql_date_range_week,
      mql_date.date_id                             AS mql_date_range_id,
  
    --opp_create_date fields
      opp_create_date.fiscal_year                     AS opportunity_created_date_range_year,
      opp_create_date.fiscal_quarter_name_fy          AS opportunity_created_date_range_quarter,
      DATE_TRUNC(month, opp_create_date.date_actual)  AS opportunity_created_date_range_month,
      opp_create_date.first_day_of_week               AS opportunity_created_date_range_week,
      opp_create_date.date_id                         AS opportunity_created_date_range_id,
  
    --sao_date fields
      sao_date.fiscal_year                     AS sao_date_range_year,
      sao_date.fiscal_quarter_name_fy          AS sao_date_range_quarter,
      DATE_TRUNC(month, sao_date.date_actual)  AS sao_date_range_month,
      sao_date.first_day_of_week               AS sao_date_range_week,
      sao_date.date_id                         AS sao_date_range_id,

    --pipeline_created_date fields
      pipeline_created_date.fiscal_year                     AS pipeline_created_date_range_year,
      pipeline_created_date.fiscal_quarter_name_fy          AS pipeline_created_date_range_quarter,
      DATE_TRUNC(month, pipeline_created_date.date_actual)  AS pipeline_created_date_range_month,
      pipeline_created_date.first_day_of_week               AS pipeline_created_date_range_week,
      pipeline_created_date.date_id                         AS pipeline_created_date_range_id,
  
    --closed_date fields
      closed_date.fiscal_year                     AS closed_date_range_year,
      closed_date.fiscal_quarter_name_fy          AS closed_date_range_quarter,
      DATE_TRUNC(month, closed_date.date_actual)  AS closed_date_range_month,
      closed_date.first_day_of_week               AS closed_date_range_week,
      closed_date.date_id                         AS closed_date_range_id,

    --bizible_date fields
      bizible_date.fiscal_year                     AS bizible_date_range_year,
      bizible_date.fiscal_quarter_name_fy          AS bizible_date_range_quarter,
      DATE_TRUNC(month, bizible_date.date_actual)  AS bizible_date_range_month,
      bizible_date.first_day_of_week               AS bizible_date_range_week,
      bizible_date.date_id                         AS bizible_date_range_id
  FROM cohort_base_combined
  CROSS JOIN today
  LEFT JOIN dim_campaign
    ON cohort_base_combined.dim_campaign_id = dim_campaign.dim_campaign_id
  LEFT JOIN fct_campaign
    ON cohort_base_combined.dim_campaign_id = fct_campaign.dim_campaign_id
  LEFT JOIN dim_crm_user user
    ON cohort_base_combined.dim_crm_user_id = user.dim_crm_user_id
  LEFT JOIN dim_crm_user manager
    ON user.manager_id = manager.dim_crm_user_id
  LEFT JOIN dim_crm_user campaign_owner
    ON fct_campaign.campaign_owner_id = campaign_owner.dim_crm_user_id
  LEFT JOIN dim_crm_user campaign_owner_manager
    ON campaign_owner.manager_id = campaign_owner_manager.dim_crm_user_id
  LEFT JOIN dim_crm_account
    ON cohort_base_combined.dim_crm_account_id=dim_crm_account.dim_crm_account_id
  LEFT JOIN dim_crm_user account_owner
    ON dim_crm_account.dim_crm_user_id = account_owner.dim_crm_user_id
  LEFT JOIN dim_crm_user opportunity_owner
    ON cohort_base_combined.opp_dim_crm_user_id = opportunity_owner.dim_crm_user_id
  LEFT JOIN person_history_final
    ON cohort_base_combined.dim_crm_touchpoint_id = person_history_final.dim_crm_touchpoint_id 
    AND cohort_base_combined.sfdc_record_id = person_history_final.sfdc_record_id
  LEFT JOIN dim_date inquiry_date
    ON cohort_base_combined.true_inquiry_date = inquiry_date.date_day
  LEFT JOIN dim_date mql_date
    ON cohort_base_combined.mql_date_latest_pt = mql_date.date_day
  LEFT JOIN dim_date opp_create_date
    ON cohort_base_combined.opp_created_date = opp_create_date.date_day
  LEFT JOIN dim_date sao_date
    ON cohort_base_combined.sales_accepted_date = sao_date.date_day
  LEFT JOIN dim_date closed_date
    ON cohort_base_combined.close_date=closed_date.date_day
  LEFT JOIN dim_date pipeline_created_date
    ON cohort_base_combined.pipeline_created_date=pipeline_created_date.date_day
  LEFT JOIN dim_date bizible_date
    ON cohort_base_combined.bizible_touchpoint_date=bizible_date.date_day

), final AS (

    SELECT  *
    FROM intermediate

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@degan",
    created_date="2022-07-05",
    updated_date="2024-05-14",
  ) }}
