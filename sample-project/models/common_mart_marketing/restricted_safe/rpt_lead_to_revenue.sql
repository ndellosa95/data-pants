{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('person_base','mart_crm_person'),
    ('dim_crm_person','dim_crm_person'),
    ('mart_crm_opportunity_stamped_hierarchy_hist', 'mart_crm_opportunity_stamped_hierarchy_hist'), 
    ('map_alternative_lead_demographics','map_alternative_lead_demographics'),
    ('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('mart_crm_account', 'mart_crm_account'),
    ('dim_date','dim_date')
]) }}

, person_base_with_tp AS (

    SELECT DISTINCT
  --IDs
      person_base.dim_crm_person_id,
      person_base.dim_crm_account_id,
      mart_crm_account.dim_parent_crm_account_id,
      dim_crm_person.sfdc_record_id,
      mart_crm_touchpoint.dim_crm_touchpoint_id,
      mart_crm_touchpoint.dim_campaign_id,
      person_base.dim_crm_user_id,
  
  --Person Data
      person_base.email_hash,
      person_base.email_domain_type,
      person_base.source_buckets,
      person_base.true_inquiry_date,
      person_base.mql_date_first_pt,
      person_base.mql_date_latest_pt,
      person_base.accepted_date,
      person_base.status,
      person_base.lead_source,
      person_base.is_inquiry,
      person_base.is_mql,
      person_base.account_demographics_sales_segment,
      person_base.account_demographics_region,
      person_base.account_demographics_geo,
      person_base.account_demographics_area,
      person_base.account_demographics_upa_country,
      person_base.account_demographics_territory,
      person_base.traction_first_response_time,
      person_base.traction_first_response_time_seconds,
      person_base.traction_response_time_in_business_hours,
      mart_crm_account.is_first_order_available,
      person_base.is_bdr_sdr_worked,
      CASE
        WHEN person_base.is_first_order_person = TRUE 
          THEN '1. New - First Order'
        ELSE '3. Growth'
      END AS person_order_type,
      person_base.lead_score_classification,
      person_base.is_defaulted_trial,

  --Account Data
      mart_crm_account.crm_account_name,
      mart_crm_account.parent_crm_account_name,
      mart_crm_account.parent_crm_account_lam,
      mart_crm_account.parent_crm_account_lam_dev_count,
      map_alternative_lead_demographics.employee_count_segment_custom,
      map_alternative_lead_demographics.employee_bucket_segment_custom,
      COALESCE(map_alternative_lead_demographics.employee_count_segment_custom,map_alternative_lead_demographics.employee_bucket_segment_custom) AS inferred_employee_segment,
      map_alternative_lead_demographics.geo_custom,
      UPPER(map_alternative_lead_demographics.geo_custom) AS inferred_geo,

  
  --Touchpoint Data
      'Person Touchpoint' AS touchpoint_type,
      mart_crm_touchpoint.bizible_touchpoint_date,
      mart_crm_touchpoint.bizible_touchpoint_position,
      mart_crm_touchpoint.bizible_touchpoint_source,
      mart_crm_touchpoint.bizible_touchpoint_type,
      mart_crm_touchpoint.bizible_ad_campaign_name,
      mart_crm_touchpoint.bizible_ad_group_name,
      mart_crm_touchpoint.bizible_salesforce_campaign,
      mart_crm_touchpoint.bizible_form_url,
      mart_crm_touchpoint.bizible_landing_page,
      mart_crm_touchpoint.bizible_form_url_raw,
      mart_crm_touchpoint.bizible_landing_page_raw,
      mart_crm_touchpoint.bizible_marketing_channel,
      mart_crm_touchpoint.bizible_marketing_channel_path,
      mart_crm_touchpoint.bizible_medium,
      mart_crm_touchpoint.bizible_referrer_page,
      mart_crm_touchpoint.bizible_referrer_page_raw,
      mart_crm_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_touchpoint.bizible_form_page_utm_content,
      mart_crm_touchpoint.bizible_form_page_utm_budget,
      mart_crm_touchpoint.bizible_form_page_utm_allptnr,
      mart_crm_touchpoint.bizible_form_page_utm_partnerid,
      mart_crm_touchpoint.bizible_landing_page_utm_content,
      mart_crm_touchpoint.bizible_landing_page_utm_budget,
      mart_crm_touchpoint.bizible_landing_page_utm_allptnr,
      mart_crm_touchpoint.bizible_landing_page_utm_partnerid,
      mart_crm_touchpoint.campaign_rep_role_name,
      mart_crm_touchpoint.touchpoint_segment,
      mart_crm_touchpoint.gtm_motion,
      mart_crm_touchpoint.pipe_name,
      mart_crm_touchpoint.is_dg_influenced,
      mart_crm_touchpoint.is_dg_sourced,
      mart_crm_touchpoint.bizible_count_first_touch,
      mart_crm_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_touchpoint.bizible_count_u_shaped,
      mart_crm_touchpoint.is_fmm_influenced,
      mart_crm_touchpoint.is_fmm_sourced,
      mart_crm_touchpoint.bizible_count_lead_creation_touch AS new_lead_created_sum,
      mart_crm_touchpoint.count_true_inquiry AS count_true_inquiry,
      mart_crm_touchpoint.count_inquiry AS inquiry_sum, 
      mart_crm_touchpoint.pre_mql_weight AS mql_sum,
      mart_crm_touchpoint.count_accepted AS accepted_sum,
      mart_crm_touchpoint.count_net_new_mql AS new_mql_sum,
      mart_crm_touchpoint.count_net_new_accepted AS new_accepted_sum,
      mart_crm_touchpoint.touchpoint_offer_type_grouped,
      mart_crm_touchpoint.touchpoint_offer_type   
    FROM person_base
    INNER JOIN dim_crm_person
      ON person_base.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    FULL JOIN mart_crm_account
      ON person_base.dim_crm_account_id = mart_crm_account.dim_crm_account_id
    LEFT JOIN mart_crm_touchpoint
      ON mart_crm_touchpoint.email_hash = person_base.email_hash
    LEFT JOIN map_alternative_lead_demographics
      ON person_base.dim_crm_person_id=map_alternative_lead_demographics.dim_crm_person_id
  
  ), opp_base_with_batp AS (
    
    SELECT
    --IDs
      opp.dim_crm_opportunity_id,
      opp.dim_crm_account_id,
      mart_crm_account.dim_parent_crm_account_id,
      mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
      mart_crm_attribution_touchpoint.dim_campaign_id,
      opp.dim_crm_user_id AS opp_dim_crm_user_id,
    
    --Opp Data
      opp.opportunity_name,
      opp.order_type AS opp_order_type,
      opp.sdr_or_bdr,
      opp.sales_qualified_source_name,
      opp.deal_path_name,
      opp.sales_type,
      opp.sales_accepted_date,
      opp.created_date AS opp_created_date,
      opp.close_date,
      opp.is_won,
      opp.is_sao,
      opp.new_logo_count,
      opp.net_arr,
      opp.net_arr_stage_1,
      opp.xdr_net_arr_stage_1,
      opp.xdr_net_arr_stage_3,
      opp.is_net_arr_closed_deal,
      opp.is_net_arr_pipeline_created,
      opp.parent_crm_account_sales_segment,
      opp.parent_crm_account_region,
      opp.parent_crm_account_geo,
      opp.parent_crm_account_area,
      opp.parent_crm_account_territory,
      opp.crm_opp_owner_sales_segment_stamped,
      opp.crm_opp_owner_region_stamped,
      opp.crm_opp_owner_area_stamped,
      opp.crm_opp_owner_geo_stamped,
      opp.product_category,
      opp.is_eligible_age_analysis,
      opp.lead_source,
      opp.source_buckets,

      --Account Data
      mart_crm_account.crm_account_name,
      mart_crm_account.parent_crm_account_name,
      mart_crm_account.parent_crm_account_lam,
      mart_crm_account.parent_crm_account_lam_dev_count,
      opp.parent_crm_account_upa_country,
    
    -- Touchpoint Data
      'Attribution Touchpoint' AS touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_touchpoint_date,
      mart_crm_attribution_touchpoint.bizible_touchpoint_position,
      mart_crm_attribution_touchpoint.bizible_touchpoint_source,
      mart_crm_attribution_touchpoint.bizible_touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
      mart_crm_attribution_touchpoint.bizible_ad_group_name,
      mart_crm_attribution_touchpoint.bizible_salesforce_campaign,
      mart_crm_attribution_touchpoint.bizible_form_url,
      mart_crm_attribution_touchpoint.bizible_landing_page,
      mart_crm_attribution_touchpoint.bizible_form_url_raw,
      mart_crm_attribution_touchpoint.bizible_landing_page_raw,
      mart_crm_attribution_touchpoint.bizible_marketing_channel,
      mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
      mart_crm_attribution_touchpoint.bizible_medium,
      mart_crm_attribution_touchpoint.bizible_referrer_page,
      mart_crm_attribution_touchpoint.bizible_referrer_page_raw,
      mart_crm_attribution_touchpoint.bizible_form_page_utm_content,
      mart_crm_attribution_touchpoint.bizible_form_page_utm_budget,
      mart_crm_attribution_touchpoint.bizible_form_page_utm_allptnr,
      mart_crm_attribution_touchpoint.bizible_form_page_utm_partnerid,
      mart_crm_attribution_touchpoint.bizible_landing_page_utm_content,
      mart_crm_attribution_touchpoint.bizible_landing_page_utm_budget,
      mart_crm_attribution_touchpoint.bizible_landing_page_utm_allptnr,
      mart_crm_attribution_touchpoint.bizible_landing_page_utm_partnerid,
      mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_attribution_touchpoint.touchpoint_segment,
      mart_crm_attribution_touchpoint.campaign_rep_role_name,
      mart_crm_attribution_touchpoint.gtm_motion,
      mart_crm_attribution_touchpoint.pipe_name,
      mart_crm_attribution_touchpoint.is_dg_influenced,
      mart_crm_attribution_touchpoint.is_dg_sourced,
      mart_crm_attribution_touchpoint.bizible_count_first_touch,
      mart_crm_attribution_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_attribution_touchpoint.bizible_count_u_shaped,
      mart_crm_attribution_touchpoint.bizible_count_w_shaped,
      mart_crm_attribution_touchpoint.bizible_count_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_u_shaped,
      mart_crm_attribution_touchpoint.bizible_weight_w_shaped,
      mart_crm_attribution_touchpoint.bizible_weight_full_path,
      mart_crm_attribution_touchpoint.bizible_weight_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_first_touch,
      mart_crm_attribution_touchpoint.is_fmm_influenced,
      mart_crm_attribution_touchpoint.is_fmm_sourced,
      mart_crm_attribution_touchpoint.touchpoint_offer_type_grouped,
      mart_crm_attribution_touchpoint.touchpoint_offer_type,  
      mart_crm_attribution_touchpoint.touchpoint_sales_stage, 
      CASE
          WHEN mart_crm_attribution_touchpoint.dim_crm_touchpoint_id IS NOT null 
            THEN opp.dim_crm_opportunity_id
          ELSE null
      END AS influenced_opportunity_id,
      SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) AS first_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) AS u_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) AS w_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) AS full_shaped_opp_created,
      SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) AS custom_opp_created,
      SUM(mart_crm_attribution_touchpoint.l_weight) AS linear_opp_created,
      SUM(mart_crm_attribution_touchpoint.first_net_arr) AS first_net_arr,
      SUM(mart_crm_attribution_touchpoint.u_net_arr) AS u_net_arr,
      SUM(mart_crm_attribution_touchpoint.w_net_arr) AS w_net_arr,
      SUM(mart_crm_attribution_touchpoint.full_net_arr) AS full_net_arr,
      SUM(mart_crm_attribution_touchpoint.custom_net_arr) AS custom_net_arr,
      SUM(mart_crm_attribution_touchpoint.linear_net_arr) AS linear_net_arr,
      CASE
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS first_sao,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
        ELSE 0 
      END AS u_shaped_sao,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
        ELSE 0 
      END AS w_shaped_sao,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS full_shaped_sao,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS custom_sao,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
        ELSE 0 
      END AS linear_sao,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
      END AS pipeline_first_net_arr,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
        END AS pipeline_u_net_arr,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
        ELSE 0 
      END AS pipeline_w_net_arr,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
        ELSE 0 
      END AS pipeline_full_net_arr,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS pipeline_custom_net_arr,
      CASE 
        WHEN opp.is_net_arr_pipeline_created = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
        ELSE 0 
      END AS pipeline_linear_net_arr,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS won_first,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) 
        ELSE 0 
      END AS won_u,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) 
        ELSE 0 
      END AS won_w,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) 
        ELSE 0 
      END AS won_full,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS won_custom,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.l_weight) 
        ELSE 0 
      END AS won_linear,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.first_net_arr) 
        ELSE 0 
      END AS won_first_net_arr,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.u_net_arr) 
        ELSE 0 
      END AS won_u_net_arr,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.w_net_arr) 
        ELSE 0 
      END AS won_w_net_arr,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.full_net_arr) 
        ELSE 0 
      END AS won_full_net_arr,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS won_custom_net_arr,
      CASE 
        WHEN opp.is_net_arr_closed_deal = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.linear_net_arr) 
        ELSE 0 
      END AS won_linear_net_arr
    FROM mart_crm_opportunity_stamped_hierarchy_hist opp
    LEFT JOIN mart_crm_attribution_touchpoint
      ON opp.dim_crm_opportunity_id=mart_crm_attribution_touchpoint.dim_crm_opportunity_id
    FULL JOIN mart_crm_account
      ON opp.dim_crm_account_id=mart_crm_account.dim_crm_account_id
    WHERE opp.created_date >= '2021-02-01'
      OR opp.created_date IS NULL
    {{dbt_utils.group_by(n=89)}}
    
), cohort_base_combined AS (
  
    SELECT
   --IDs
      dim_crm_person_id,
      person_base_with_tp.dim_crm_account_id,
      sfdc_record_id,
      COALESCE (person_base_with_tp.dim_crm_touchpoint_id,opp_base_with_batp.dim_crm_touchpoint_id) AS dim_crm_touchpoint_id, 
      person_base_with_tp.dim_crm_touchpoint_id AS dim_crm_btp_touchpoint_id,
      opp_base_with_batp.dim_crm_touchpoint_id AS dim_crm_batp_touchpoint_id,
      dim_crm_opportunity_id,
      COALESCE (person_base_with_tp.dim_campaign_id,opp_base_with_batp.dim_campaign_id) AS dim_campaign_id, 
      dim_crm_user_id,
      opp_dim_crm_user_id,
  
  --Person Data
      email_hash,
      email_domain_type,
      person_base_with_tp.source_buckets,
      true_inquiry_date,
      mql_date_first_pt,
      mql_date_latest_pt,
      accepted_date,
      status,
      person_base_with_tp.lead_source,
      is_inquiry,
      is_mql,
      person_base_with_tp.account_demographics_sales_segment,
      person_base_with_tp.account_demographics_region,
      person_base_with_tp.account_demographics_geo,
      person_base_with_tp.account_demographics_area,
      person_base_with_tp.account_demographics_upa_country,
      person_base_with_tp.account_demographics_territory,
      is_first_order_available,
      is_bdr_sdr_worked,
      person_order_type,
      employee_count_segment_custom,
      employee_bucket_segment_custom,
      inferred_employee_segment,
      geo_custom,
      inferred_geo,
      traction_first_response_time,
      traction_first_response_time_seconds,
      traction_response_time_in_business_hours,
      lead_score_classification,
      is_defaulted_trial,
  
  --Opp Data
      opportunity_name,
      opp_order_type,
      sdr_or_bdr,
      sales_qualified_source_name,
      deal_path_name,
      sales_type,
      sales_accepted_date,
      opp_created_date,
      close_date,
      opp_base_with_batp.lead_source AS opp_lead_source,
      opp_base_with_batp.source_buckets AS opp_source_buckets,
      is_won,
      is_sao,
      new_logo_count,
      net_arr,
      net_arr_stage_1,
      xdr_net_arr_stage_1,
      xdr_net_arr_stage_3,
      is_net_arr_closed_deal,
      is_net_arr_pipeline_created,
      is_eligible_age_analysis,
      opp_base_with_batp.parent_crm_account_sales_segment AS opp_account_demographics_sales_segment,
      opp_base_with_batp.parent_crm_account_region AS opp_account_demographics_region,
      opp_base_with_batp.parent_crm_account_geo AS opp_account_demographics_geo,
      opp_base_with_batp.parent_crm_account_territory AS opp_account_demographics_territory,
      opp_base_with_batp.parent_crm_account_area AS opp_account_demographics_area,
      crm_opp_owner_sales_segment_stamped,
      crm_opp_owner_region_stamped,
      crm_opp_owner_area_stamped,
      crm_opp_owner_geo_stamped,
      product_category,

  --Account Data
      COALESCE(person_base_with_tp.crm_account_name,opp_base_with_batp.crm_account_name) AS crm_account_name,
      COALESCE(person_base_with_tp.parent_crm_account_name,opp_base_with_batp.parent_crm_account_name) AS parent_crm_account_name,
      COALESCE(person_base_with_tp.parent_crm_account_lam,opp_base_with_batp.parent_crm_account_lam) AS parent_crm_account_lam,
      COALESCE(person_base_with_tp.parent_crm_account_lam_dev_count,opp_base_with_batp.parent_crm_account_lam_dev_count) AS parent_crm_account_lam_dev_count,
      parent_crm_account_upa_country,
      parent_crm_account_territory,
  
  --Touchpoint Data
      COALESCE(person_base_with_tp.touchpoint_offer_type_grouped,opp_base_with_batp.touchpoint_offer_type_grouped) AS touchpoint_offer_type_grouped, 
      COALESCE(person_base_with_tp.touchpoint_offer_type,opp_base_with_batp.touchpoint_offer_type) AS touchpoint_offer_type,
      opp_base_with_batp.touchpoint_offer_type_grouped AS opp_touchpoint_offer_type_grouped,
      opp_base_with_batp.touchpoint_offer_type AS opp_touchpoint_offer_type, 
      opp_base_with_batp.touchpoint_sales_stage AS opp_touchpoint_sales_stage,
      COALESCE(person_base_with_tp.bizible_touchpoint_date,opp_base_with_batp.bizible_touchpoint_date) AS bizible_touchpoint_date, 
      COALESCE(person_base_with_tp.campaign_rep_role_name,opp_base_with_batp.campaign_rep_role_name) AS campaign_rep_role_name, 
      COALESCE(person_base_with_tp.bizible_touchpoint_position,opp_base_with_batp.bizible_touchpoint_position) AS bizible_touchpoint_position, 
      COALESCE(person_base_with_tp.bizible_touchpoint_source,opp_base_with_batp.bizible_touchpoint_source) AS bizible_touchpoint_source, 
      COALESCE(person_base_with_tp.bizible_touchpoint_type,opp_base_with_batp.bizible_touchpoint_type) AS bizible_touchpoint_type, 
      COALESCE(person_base_with_tp.bizible_ad_campaign_name,opp_base_with_batp.bizible_ad_campaign_name) AS bizible_ad_campaign_name, 
      COALESCE(person_base_with_tp.bizible_ad_group_name,opp_base_with_batp.bizible_ad_group_name) AS bizible_ad_group_name, 
      COALESCE(person_base_with_tp.bizible_salesforce_campaign,opp_base_with_batp.bizible_salesforce_campaign) AS bizible_salesforce_campaign, 
      COALESCE(person_base_with_tp.bizible_form_url,opp_base_with_batp.bizible_form_url) AS bizible_form_url, 
      COALESCE(person_base_with_tp.bizible_landing_page,opp_base_with_batp.bizible_landing_page) AS bizible_landing_page, 
      COALESCE(person_base_with_tp.bizible_form_url_raw,opp_base_with_batp.bizible_form_url_raw) AS bizible_form_url_raw, 
      COALESCE(person_base_with_tp.bizible_landing_page_raw,opp_base_with_batp.bizible_landing_page_raw) AS bizible_landing_page_raw, 
      COALESCE(person_base_with_tp.bizible_marketing_channel,opp_base_with_batp.bizible_marketing_channel) AS bizible_marketing_channel, 
      opp_base_with_batp.bizible_marketing_channel AS opp_bizible_marketing_channel,
      COALESCE(person_base_with_tp.bizible_marketing_channel_path,opp_base_with_batp.bizible_marketing_channel_path) AS bizible_marketing_channel_path, 
      opp_base_with_batp.bizible_marketing_channel_path AS opp_bizible_marketing_channel_path,
      COALESCE(person_base_with_tp.bizible_medium,opp_base_with_batp.bizible_medium) AS bizible_medium, 
      COALESCE(person_base_with_tp.bizible_referrer_page,opp_base_with_batp.bizible_referrer_page) AS bizible_referrer_page, 
      COALESCE(person_base_with_tp.bizible_referrer_page_raw,opp_base_with_batp.bizible_referrer_page_raw) AS bizible_referrer_page_raw, 
      COALESCE(person_base_with_tp.bizible_integrated_campaign_grouping,opp_base_with_batp.bizible_integrated_campaign_grouping) AS bizible_integrated_campaign_grouping, 
      COALESCE(person_base_with_tp.touchpoint_segment,opp_base_with_batp.touchpoint_segment) AS touchpoint_segment, 
      COALESCE(person_base_with_tp.gtm_motion,opp_base_with_batp.gtm_motion) AS gtm_motion, 
      COALESCE(person_base_with_tp.pipe_name,opp_base_with_batp.pipe_name) AS pipe_name, 
      COALESCE(person_base_with_tp.is_dg_influenced,opp_base_with_batp.is_dg_influenced) AS is_dg_influenced, 
      COALESCE(person_base_with_tp.is_dg_sourced,opp_base_with_batp.is_dg_sourced) AS is_dg_sourced, 
      COALESCE(person_base_with_tp.bizible_count_first_touch,opp_base_with_batp.bizible_count_first_touch) AS bizible_count_first_touch, 
      COALESCE(person_base_with_tp.bizible_count_lead_creation_touch,opp_base_with_batp.bizible_count_lead_creation_touch) AS bizible_count_lead_creation_touch, 
      COALESCE(person_base_with_tp.bizible_count_u_shaped,opp_base_with_batp.bizible_count_u_shaped) AS bizible_count_u_shaped, 
      COALESCE(person_base_with_tp.is_fmm_influenced,opp_base_with_batp.is_fmm_influenced) AS is_fmm_influenced, 
      COALESCE(person_base_with_tp.is_fmm_sourced,opp_base_with_batp.is_fmm_sourced) AS is_fmm_sourced,
      COALESCE(person_base_with_tp.bizible_form_page_utm_content,opp_base_with_batp.bizible_form_page_utm_content) AS bizible_form_page_utm_content, 
      COALESCE(person_base_with_tp.bizible_form_page_utm_budget,opp_base_with_batp.bizible_form_page_utm_budget) AS bizible_form_page_utm_budget, 
      COALESCE(person_base_with_tp.bizible_form_page_utm_allptnr,opp_base_with_batp.bizible_form_page_utm_allptnr) AS bizible_form_page_utm_allptnr, 
      COALESCE(person_base_with_tp.bizible_form_page_utm_partnerid,opp_base_with_batp.bizible_form_page_utm_partnerid) AS bizible_form_page_utm_partnerid, 
      COALESCE(person_base_with_tp.bizible_landing_page_utm_content,opp_base_with_batp.bizible_landing_page_utm_content) AS bizible_landing_page_utm_content, 
      COALESCE(person_base_with_tp.bizible_landing_page_utm_budget,opp_base_with_batp.bizible_landing_page_utm_budget) AS bizible_landing_page_utm_budget, 
      COALESCE(person_base_with_tp.bizible_landing_page_utm_allptnr,opp_base_with_batp.bizible_landing_page_utm_allptnr) AS bizible_landing_page_utm_allptnr, 
      COALESCE(person_base_with_tp.bizible_landing_page_utm_partnerid,opp_base_with_batp.bizible_landing_page_utm_partnerid) AS bizible_landing_page_utm_partnerid, 
      new_lead_created_sum,
      count_true_inquiry,
      inquiry_sum, 
      mql_sum,
      accepted_sum,
      new_mql_sum,
      new_accepted_sum,
      bizible_count_w_shaped,
      bizible_count_custom_model,
      bizible_weight_custom_model,
      bizible_weight_u_shaped,
      bizible_weight_w_shaped,
      bizible_weight_full_path,
      bizible_weight_first_touch,
      influenced_opportunity_id,
      first_opp_created,
      u_shaped_opp_created,
      w_shaped_opp_created,
      full_shaped_opp_created,
      custom_opp_created,
      linear_opp_created,
      first_net_arr,
      u_net_arr,
      w_net_arr,
      full_net_arr,
      custom_net_arr,
      linear_net_arr,
      first_sao,
      u_shaped_sao,
      w_shaped_sao,
      full_shaped_sao,
      custom_sao,
      linear_sao,
      pipeline_first_net_arr,
      pipeline_u_net_arr,
      pipeline_w_net_arr,
      pipeline_full_net_arr,
      pipeline_custom_net_arr,
      pipeline_linear_net_arr,
      won_first,
      won_u,
      won_w,
      won_full,
      won_custom,
      won_linear,
      won_first_net_arr,
      won_u_net_arr,
      won_w_net_arr,
      won_full_net_arr,
      won_custom_net_arr,
      won_linear_net_arr
  FROM person_base_with_tp
  FULL JOIN opp_base_with_batp
    ON person_base_with_tp.dim_crm_account_id=opp_base_with_batp.dim_crm_account_id

), intermediate AS (

  SELECT DISTINCT
    cohort_base_combined.*,
    --inquiry_date fields
    inquiry_date.fiscal_year                     AS inquiry_date_range_year,
    inquiry_date.fiscal_quarter_name_fy          AS inquiry_date_range_quarter,
    DATE_TRUNC(month, inquiry_date.date_actual)  AS inquiry_date_range_month,
    inquiry_date.first_day_of_week               AS inquiry_date_range_week,
    inquiry_date.date_id                         AS inquiry_date_range_id,
  
    --mql_date fields
    mql_date.fiscal_year                     AS mql_date_range_year,
    mql_date.fiscal_quarter_name_fy          AS mql_date_range_quarter,
    DATE_TRUNC(month, mql_date.date_actual)  AS mql_date_range_month,
    mql_date.first_day_of_week               AS mql_date_range_week,
    mql_date.date_id                         AS mql_date_range_id,
  
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
  
    --closed_date fields
    closed_date.fiscal_year                     AS closed_date_range_year,
    closed_date.fiscal_quarter_name_fy          AS closed_date_range_quarter,
    DATE_TRUNC(month, closed_date.date_actual)  AS closed_date_range_month,
    closed_date.first_day_of_week               AS closed_date_range_week,
    closed_date.date_id                         AS closed_date_range_id,

    --touchpoint_date fields
    touchpoint_date.fiscal_year                     AS touchpoint_date_range_year,
    touchpoint_date.fiscal_quarter_name_fy          AS touchpoint_date_range_quarter,
    DATE_TRUNC(month, touchpoint_date.date_actual)  AS touchpoint_date_range_month,
    touchpoint_date.first_day_of_week               AS touchpoint_date_range_week,
    touchpoint_date.date_id                         AS touchpoint_date_range_id
  FROM cohort_base_combined
  LEFT JOIN dim_date AS inquiry_date
    ON cohort_base_combined.true_inquiry_date = inquiry_date.date_day
  LEFT JOIN dim_date AS mql_date
    ON cohort_base_combined.mql_date_latest_pt = mql_date.date_day
  LEFT JOIN dim_date AS opp_create_date
    ON cohort_base_combined.opp_created_date = opp_create_date.date_day
  LEFT JOIN dim_date AS sao_date
    ON cohort_base_combined.sales_accepted_date = sao_date.date_day
  LEFT JOIN dim_date AS closed_date
    ON cohort_base_combined.close_date = closed_date.date_day
  LEFT JOIN dim_date AS touchpoint_date
    ON cohort_base_combined.bizible_touchpoint_date = touchpoint_date.date_day

), final AS (

    SELECT DISTINCT *
    FROM intermediate

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-10-05",
    updated_date="2024-05-07",
  ) }}