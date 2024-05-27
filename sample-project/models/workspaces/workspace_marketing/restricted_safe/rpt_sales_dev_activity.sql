{{ config(materialized='table') }}

{{ simple_cte([
    ('mart_crm_opportunity_stamped_hierarchy_hist','mart_crm_opportunity_stamped_hierarchy_hist'),
    ('dim_crm_user','dim_crm_user'),
    ('mart_crm_person','mart_crm_person'),
    ('sfdc_lead','sfdc_lead'),
    ('mart_crm_account','mart_crm_account'),
    ('mart_crm_event','mart_crm_event'),
    ('mart_crm_task','mart_crm_task'),
    ('mart_team_member_directory','mart_team_member_directory'),
    ('bdg_crm_opportunity_contact_role','bdg_crm_opportunity_contact_role'),
    ('dim_date', 'dim_date')
  ]) 
}}

, sales_dev_opps AS (

  SELECT 
    mart_crm_opportunity_stamped_hierarchy_hist.dim_crm_account_id,
    mart_crm_account.bdr_prospecting_status,
    mart_crm_opportunity_stamped_hierarchy_hist.dim_crm_opportunity_id,
    mart_crm_opportunity_stamped_hierarchy_hist.net_arr,
    mart_crm_opportunity_stamped_hierarchy_hist.net_arr_stage_1,
    mart_crm_opportunity_stamped_hierarchy_hist.xdr_net_arr_stage_1,
    mart_crm_opportunity_stamped_hierarchy_hist.xdr_net_arr_stage_3,
    mart_crm_opportunity_stamped_hierarchy_hist.sales_accepted_date AS sales_accepted_date,
    mart_crm_opportunity_stamped_hierarchy_hist.sales_accepted_fiscal_quarter_name,
    dim_date.day_of_fiscal_quarter AS sao_day_of_fiscal_quarter,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_0_pending_acceptance_date,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_0_pending_acceptance_month,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_0_pending_acceptance_fiscal_quarter_name,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_1_discovery_date,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_1_discovery_month,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_1_discovery_fiscal_quarter_name,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_2_scoping_date,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_2_scoping_month,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_2_scoping_fiscal_quarter_name,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_3_technical_evaluation_date,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_3_technical_evaluation_month,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_3_technical_evaluation_fiscal_quarter_name, 
    mart_crm_opportunity_stamped_hierarchy_hist.days_in_1_discovery,
    mart_crm_opportunity_stamped_hierarchy_hist.days_in_sao,
    mart_crm_opportunity_stamped_hierarchy_hist.days_since_last_activity,
    mart_crm_opportunity_stamped_hierarchy_hist.sales_qualified_source_name,
    mart_crm_opportunity_stamped_hierarchy_hist.report_opportunity_user_segment,
    mart_crm_opportunity_stamped_hierarchy_hist.report_opportunity_user_geo,
    mart_crm_opportunity_stamped_hierarchy_hist.report_opportunity_user_region,
    mart_crm_opportunity_stamped_hierarchy_hist.report_opportunity_user_area,
    mart_crm_opportunity_stamped_hierarchy_hist.report_user_segment_geo_region_area,
    mart_crm_opportunity_stamped_hierarchy_hist.parent_crm_account_territory,
    mart_crm_opportunity_stamped_hierarchy_hist.parent_crm_account_sales_segment,
    mart_crm_opportunity_stamped_hierarchy_hist.parent_crm_account_geo,
    mart_crm_opportunity_stamped_hierarchy_hist.parent_crm_account_region,
    mart_crm_opportunity_stamped_hierarchy_hist.parent_crm_account_area,
    mart_crm_opportunity_stamped_hierarchy_hist.deal_path_name,
    mart_crm_opportunity_stamped_hierarchy_hist.created_date AS opp_created_date,
    mart_crm_opportunity_stamped_hierarchy_hist.close_date,
    mart_crm_opportunity_stamped_hierarchy_hist.pipeline_created_date,
    mart_crm_opportunity_stamped_hierarchy_hist.order_type,
    mart_crm_opportunity_stamped_hierarchy_hist.stage_name,
    mart_crm_opportunity_stamped_hierarchy_hist.product_category,
    mart_crm_opportunity_stamped_hierarchy_hist.product_details,
    mart_crm_opportunity_stamped_hierarchy_hist.products_purchased,
    mart_crm_opportunity_stamped_hierarchy_hist.crm_account_focus_account, 
    mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_sales_segment_stamped,
    mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_business_unit_stamped,
    mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_geo_stamped,
    mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_region_stamped,
    mart_crm_opportunity_stamped_hierarchy_hist.crm_opp_owner_area_stamped,
    mart_crm_opportunity_stamped_hierarchy_hist.is_sao,
    mart_crm_opportunity_stamped_hierarchy_hist.is_net_arr_closed_deal,
    mart_crm_opportunity_stamped_hierarchy_hist.is_net_arr_pipeline_created,
    mart_crm_opportunity_stamped_hierarchy_hist.is_eligible_age_analysis,
    mart_crm_opportunity_stamped_hierarchy_hist.is_eligible_open_pipeline,
    mart_crm_opportunity_stamped_hierarchy_hist.opportunity_business_development_representative,
    mart_crm_opportunity_stamped_hierarchy_hist.opportunity_sales_development_representative,
    CASE 
    WHEN mart_crm_opportunity_stamped_hierarchy_hist.opportunity_business_development_representative IS NOT NULL 
    THEN 'BDR' 
    WHEN mart_crm_opportunity_stamped_hierarchy_hist.opportunity_sales_development_representative IS NOT NULL 
    THEN 'SDR' 
    END AS sales_dev_bdr_or_sdr,
    COALESCE(opportunity_business_development_representative,opportunity_sales_development_representative) AS sdr_bdr_user_id
    FROM mart_crm_opportunity_stamped_hierarchy_hist
    LEFT JOIN dim_date 
      ON mart_crm_opportunity_stamped_hierarchy_hist.sales_accepted_date = dim_date.date_day
    LEFT JOIN mart_crm_account 
      ON mart_crm_opportunity_stamped_hierarchy_hist.dim_crm_account_id = mart_crm_account.dim_crm_account_id
    WHERE sdr_bdr_user_id IS NOT NULL 
      AND stage_1_discovery_date >= '2022-02-01' 


), sales_dev_hierarchy AS (
   SELECT DISTINCT 
   --Sales Dev Data
    sales_dev_rep.dim_crm_user_id AS sales_dev_rep_user_id, 
    sales_dev_rep.user_role_name AS sales_dev_rep_role_name,
    COALESCE(rep.full_name, sales_dev_rep.user_name)               AS sales_dev_rep_user_name,
    sales_dev_rep.title AS sales_dev_rep_title,
    sales_dev_rep.department AS sales_dev_rep_department,
    sales_dev_rep.team AS sales_dev_rep_team,
    sales_dev_rep.is_active AS sales_dev_rep_is_active,
    sales_dev_rep.crm_user_sales_segment,
    sales_dev_rep.crm_user_geo,
    sales_dev_rep.crm_user_region,
    sales_dev_rep.crm_user_area,
    sales_dev_rep.crm_user_business_unit,
    sales_dev_rep.employee_number, 
    sales_dev_rep.user_email AS sales_dev_rep_email,
 -- manager_data 
    sales_dev_rep.manager_id AS sales_dev_rep_direct_manager_id,
    COALESCE(manager.full_name, sales_dev_manager.user_name) AS sales_dev_rep_direct_manager_name,
    sales_dev_manager.user_email AS sales_dev_manager_email,
    sales_dev_manager.user_role_name AS sales_dev_manager_role_name,
  --Leader Data
    sales_dev_manager.manager_id AS sales_dev_rep_manager_id,
    COALESCE(leader.full_name, sales_dev_leader.user_name) AS sales_dev_rep_manager_name,
    sales_dev_leader.user_email AS sales_dev_leader_email,
    sales_dev_leader.user_role_name AS sales_dev_leader_role_name

  FROM dim_crm_user sales_dev_rep

  INNER JOIN sales_dev_opps 
    ON sales_dev_rep.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id

  LEFT JOIN dim_crm_user sales_dev_manager 
    ON sales_dev_rep.manager_id = sales_dev_manager.dim_crm_user_id  

  LEFT JOIN dim_crm_user sales_dev_leader 
    ON sales_dev_manager.manager_id = sales_dev_leader.dim_crm_user_id 

  LEFT JOIN mart_team_member_directory AS rep 
    ON sales_dev_rep_email = rep.work_email

  LEFT JOIN mart_team_member_directory AS manager 
    ON sales_dev_manager_email = manager.work_email

  LEFT JOIN mart_team_member_directory AS leader 
    ON sales_dev_leader_email = leader.work_email


), merged_person_base AS (

  SELECT DISTINCT  
    mart_crm_person.dim_crm_person_id,
    sfdc_lead.converted_contact_id AS sfdc_record_id,
    sfdc_lead.lead_id AS original_lead_id,
    sales_dev_opps.dim_crm_opportunity_id,
    sales_dev_opps.opp_created_date,
    mart_crm_person.dim_crm_account_id
  FROM sfdc_lead 
  LEFT JOIN mart_crm_person 
    ON mart_crm_person.sfdc_record_id = sfdc_lead.converted_contact_id  
  LEFT JOIN sales_dev_opps 
    ON converted_opportunity_id = dim_crm_opportunity_id 
  WHERE converted_contact_id IS NOT NULL 

), contacts_on_opps AS (

  SELECT 
    bdg_crm_opportunity_contact_role.sfdc_record_id AS sfdc_record_id,
    bdg_crm_opportunity_contact_role.dim_crm_person_id AS dim_crm_person_id,
    bdg_crm_opportunity_contact_role.contact_role,
    bdg_crm_opportunity_contact_role.is_primary_contact,
    sales_dev_opps.dim_crm_opportunity_id,
    sales_dev_opps.opp_created_date AS opp_created_date,
    sales_dev_opps.sales_accepted_date AS sales_accepted_date,
    sales_dev_opps.sdr_bdr_user_id AS dim_crm_user_id 
  FROM bdg_crm_opportunity_contact_role
  INNER JOIN sales_dev_opps
    ON sales_dev_opps.dim_crm_opportunity_id = bdg_crm_opportunity_contact_role.dim_crm_opportunity_id
  
), activity_base AS (

  SELECT 
    mart_crm_event.event_id AS activity_id,
    mart_crm_event.dim_crm_user_id,
    mart_crm_event.dim_crm_opportunity_id,
    mart_crm_event.dim_crm_account_id,
    mart_crm_event.sfdc_record_id,
    mart_crm_event.dim_crm_person_id,
    dim_crm_user.dim_crm_user_id AS booked_by_user_id,
    mart_crm_event.event_date AS activity_date,
    dim_date.day_of_fiscal_quarter as activity_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy as activity_fiscal_quarter_name,
    'Event' AS activity_type,
    mart_crm_event.event_type AS activity_subtype
  FROM mart_crm_event
  LEFT JOIN dim_crm_user 
    ON booked_by_employee_number = dim_crm_user.employee_number 
    LEFT JOIN dim_date 
    ON mart_crm_event.event_date = dim_date.date_day
  INNER JOIN sales_dev_opps 
    ON mart_crm_event.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
      OR booked_by_user_id = sales_dev_opps.sdr_bdr_user_id 
  WHERE activity_date >= '2022-01-01'
  UNION
  SELECT 
    mart_crm_task.task_id AS activity_id,
    mart_crm_task.dim_crm_user_id,
    mart_crm_task.dim_crm_opportunity_id,
    mart_crm_task.dim_crm_account_id,
    mart_crm_task.sfdc_record_id,
    mart_crm_task.dim_crm_person_id,
    NULL AS booked_by_user_id,
    mart_crm_task.task_completed_date AS activity_date,
    dim_date.day_of_fiscal_quarter as activity_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy as activity_fiscal_quarter_name,
    mart_crm_task.task_type AS activity_type,
    mart_crm_task.task_subtype AS activity_subtype
  FROM mart_crm_task
  INNER JOIN sales_dev_opps 
    ON mart_crm_task.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
  LEFT JOIN dim_date 
    ON mart_crm_task.task_completed_date = dim_date.date_day
  WHERE activity_date >= '2022-01-01'

), activity_final AS (

  SELECT DISTINCT 
    activity_base.activity_id,
    COALESCE(activity_base.booked_by_user_id,activity_base.dim_crm_user_id) AS dim_crm_user_id,
    mart_crm_person.dim_crm_person_id,
    COALESCE(mart_crm_person.sfdc_record_id, activity_base.sfdc_record_id) AS sfdc_record_id,
    COALESCE(mart_crm_person.dim_crm_account_id, activity_base.dim_crm_account_id) AS dim_crm_account_id,
    activity_base.activity_date::DATE AS activity_date,
    activity_base.activity_day_of_fiscal_quarter,
    activity_base.activity_fiscal_quarter_name,
    activity_base.activity_type,
    activity_base.activity_subtype
  FROM activity_base
  LEFT JOIN merged_person_base 
    ON activity_base.sfdc_record_id = merged_person_base.original_lead_id
  LEFT JOIN mart_crm_person 
    ON COALESCE(merged_person_base.dim_crm_person_id,activity_base.dim_crm_person_id) = mart_crm_person.dim_crm_person_id    
  
), opp_to_lead AS (

  SELECT DISTINCT
    sales_dev_opps.*,
    merged_person_base.dim_crm_person_id AS converted_person_id,
    contacts_on_opps.dim_crm_person_id AS contact_person_id,
    activity_final.dim_crm_person_id AS activity_person_id,
    COALESCE(merged_person_base.dim_crm_person_id,contacts_on_opps.dim_crm_person_id,activity_final.dim_crm_person_id) AS waterfall_person_id,
    COALESCE(DATEDIFF(DAY,activity_date,sales_dev_opps.sales_accepted_date),0) AS activity_to_sao_days
  FROM sales_dev_opps
  LEFT JOIN merged_person_base 
    ON sales_dev_opps.dim_crm_opportunity_id = merged_person_base.dim_crm_opportunity_id
  LEFT JOIN contacts_on_opps 
    ON sales_dev_opps.dim_crm_opportunity_id = contacts_on_opps.dim_crm_opportunity_id
  LEFT JOIN activity_final
    ON sales_dev_opps.dim_crm_account_id = activity_final.dim_crm_account_id 
      AND activity_final.activity_date <= sales_dev_opps.sales_accepted_date 
      AND sales_dev_opps.sdr_bdr_user_id = activity_final.dim_crm_user_id

), opps_missing_link AS (

  SELECT * 
  FROM opp_to_lead 
  WHERE waterfall_person_id IS NULL OR activity_to_sao_days > 90 --adds back in the opps that are being discarded due to a too long delay from activity on the lead for that lead to be credited with SAO creation

), final AS (

  SELECT 
    mart_crm_person.dim_crm_person_id,
    mart_crm_person.sfdc_record_id,
    COALESCE(opp_to_lead.dim_crm_account_id,mart_crm_person.dim_crm_account_id) AS dim_crm_account_id,
    COALESCE(opp_to_lead.bdr_prospecting_status,mart_crm_person.matched_account_bdr_prospecting_status) AS bdr_prospecting_status,
    mart_crm_person.mql_date_latest,
    dim_mql_date.day_of_fiscal_quarter AS mql_day_of_fiscal_quarter,
    dim_mql_date.fiscal_quarter_name_fy AS mql_fiscal_quarter_name,
    mart_crm_person.inquiry_date_pt,
    mart_crm_person.true_inquiry_date,
    dim_inquiry_date.day_of_fiscal_quarter as inquiry_day_of_fiscal_quarter,
    dim_inquiry_date.fiscal_quarter_name_fy as inquiry_fiscal_quarter_name,
    mart_crm_person.account_demographics_sales_segment AS person_sales_segment,
    mart_crm_person.account_demographics_sales_segment_grouped AS person_sales_segment_grouped,
    mart_crm_person.account_demographics_geo as person_first_geo,
    mart_crm_person.is_mql,
    mart_crm_person.is_first_order_person,
    mart_crm_person.person_first_country,
    mart_crm_person.lead_score_classification,
    mart_crm_person.is_defaulted_trial,
    mart_crm_person.lead_source,
    mart_crm_person.source_buckets,
    mart_crm_person.email_domain_type,
    mart_crm_person.sfdc_record_type,
    CASE 
      WHEN mart_crm_person.propensity_to_purchase_score_group IS NULL 
        THEN 'No PTP Score' 
      ELSE mart_crm_person.propensity_to_purchase_score_group 
    END AS propensity_to_purchase_score_group,
    CASE 
      WHEN propensity_to_purchase_score_group = '4' OR propensity_to_purchase_score_group = '5' 
        THEN TRUE 
      ELSE FALSE 
    END AS is_high_ptp_lead,
    mart_crm_person.marketo_last_interesting_moment,
    mart_crm_person.marketo_last_interesting_moment_date,
    activity_final.dim_crm_user_id,
    activity_final.activity_date,
    activity_final.activity_type,
    activity_final.activity_subtype,
    activity_final.activity_id,
    activity_final.activity_day_of_fiscal_quarter,
    activity_final.activity_fiscal_quarter_name,
    NULL as tasks_completed,
    CASE 
      WHEN activity_final.activity_date >= mart_crm_person.mql_date_latest 
        THEN TRUE 
      ELSE FALSE 
    END AS worked_after_mql_flag,
    opp_to_lead.dim_crm_opportunity_id,
    opp_to_lead.sdr_bdr_user_id,
    opp_to_lead.net_arr,
    opp_to_lead.net_arr_stage_1,
    opp_to_lead.xdr_net_arr_stage_1,
    opp_to_lead.xdr_net_arr_stage_3,
    opp_to_lead.sales_accepted_date,
    opp_to_lead.sales_accepted_fiscal_quarter_name,
    opp_to_lead.sao_day_of_fiscal_quarter,
    opp_to_lead.stage_0_pending_acceptance_date,
    opp_to_lead.stage_0_pending_acceptance_month,
    opp_to_lead.stage_0_pending_acceptance_fiscal_quarter_name,
    opp_to_lead.stage_1_discovery_date,
    opp_to_lead.stage_1_discovery_month,
    opp_to_lead.stage_1_discovery_fiscal_quarter_name,
    opp_to_lead.stage_2_scoping_date,
    opp_to_lead.stage_2_scoping_month,
    opp_to_lead.stage_2_scoping_fiscal_quarter_name,
    opp_to_lead.stage_3_technical_evaluation_date,
    opp_to_lead.stage_3_technical_evaluation_month,
    opp_to_lead.stage_3_technical_evaluation_fiscal_quarter_name, 
    opp_to_lead.days_in_1_discovery,
    opp_to_lead.days_in_sao,
    opp_to_lead.days_since_last_activity,
    opp_to_lead.sales_qualified_source_name,
    opp_to_lead.report_opportunity_user_segment,
    opp_to_lead.report_opportunity_user_geo,
    opp_to_lead.report_opportunity_user_region,
    opp_to_lead.report_opportunity_user_area,
    opp_to_lead.report_user_segment_geo_region_area,
    opp_to_lead.parent_crm_account_territory,
    opp_to_lead.parent_crm_account_sales_segment,
    opp_to_lead.parent_crm_account_geo,
    opp_to_lead.parent_crm_account_region,
    opp_to_lead.parent_crm_account_area,
    opp_to_lead.deal_path_name,
    opp_to_lead.opp_created_date,
    opp_to_lead.close_date,
    opp_to_lead.pipeline_created_date,
    opp_to_lead.activity_to_sao_days,
    opp_to_lead.order_type,
    opp_to_lead.stage_name,
    opp_to_lead.product_category,
    opp_to_lead.product_details,
    opp_to_lead.products_purchased,
    opp_to_lead.crm_account_focus_account,
    opp_to_lead.sales_dev_bdr_or_sdr,
    opp_to_lead.opportunity_sales_development_representative,
    opp_to_lead.opportunity_business_development_representative,
    opp_to_lead.crm_opp_owner_sales_segment_stamped,
    opp_to_lead.crm_opp_owner_business_unit_stamped,
    opp_to_lead.crm_opp_owner_geo_stamped,
    opp_to_lead.crm_opp_owner_region_stamped,
    opp_to_lead.crm_opp_owner_area_stamped,
    opp_to_lead.is_sao,
    opp_to_lead.is_net_arr_closed_deal,
    opp_to_lead.is_net_arr_pipeline_created,
    opp_to_lead.is_eligible_age_analysis,
    opp_to_lead.is_eligible_open_pipeline,
    sales_dev_hierarchy.*
  FROM mart_crm_person
  LEFT JOIN dim_date dim_mql_date
   ON mart_crm_person.mql_date_latest = dim_mql_date.date_day 
  LEFT JOIN dim_date dim_inquiry_date
   ON mart_crm_person.true_inquiry_date = dim_inquiry_date.date_day 
  LEFT JOIN activity_final
    ON mart_crm_person.dim_crm_person_id = activity_final.dim_crm_person_id 
  LEFT JOIN opp_to_lead 
    ON mart_crm_person.dim_crm_person_id = opp_to_lead.waterfall_person_id
LEFT JOIN sales_dev_hierarchy
    ON COALESCE(opp_to_lead.sdr_bdr_user_id,activity_final.dim_crm_user_id) = sales_dev_hierarchy.sales_dev_rep_user_id 
  WHERE activity_to_sao_days <= 90 OR activity_to_sao_days IS NULL 
  UNION 
  SELECT DISTINCT -- distinct is necessary in order to not duplicate rows as addition of the rule above of activity_to_sao_days >90 might create multiple rows if there are multiple leads that satisfy the condition per opp which is not ideal. 
    NULL AS dim_crm_person_id,
    NULL AS sfdc_record_id,
    opps_missing_link.dim_crm_account_id AS dim_crm_account_id,
    opps_missing_link.bdr_prospecting_status,
    NULL AS mql_date_latest,
    NULL AS mql_day_of_fiscal_quarter,
    NULL AS mql_fiscal_quarter_name,
    NULL AS inquiry_date_pt,
    NULL AS true_inquiry_date,
    NULL AS inquiry_day_of_fiscal_quarter,
    NULL AS inquiry_fiscal_quarter_name,
    NULL AS person_sales_segment,
    NULL AS person_sales_segment_grouped,
    NULL AS person_first_geo,
    NULL AS is_mql,
    NULL AS is_first_order_person,
    NULL AS person_first_country,
    NULL AS lead_score_classification,
    NULL AS is_defaulted_trial,
    NULL AS lead_source,
    NULL AS source_buckets,
    NULL AS email_domain_type,
    NULL AS sfdc_record_type,
    NULL AS propensity_to_purchase_score_group,
    NULL AS is_high_ptp_lead,
    NULL AS marketo_last_interesting_moment,
    NULL AS marketo_last_interesting_moment_date,
    NULL AS dim_crm_user_id,
    NULL AS activity_date,
    NULL AS activity_type,
    NULL AS activity_subtype,
    NULL AS activity_id,
    NULL AS activity_day_of_fiscal_quarter,
    NULL AS activity_fiscal_quarter_name,
    NULL AS tasks_completed,
    NULL AS worked_after_mql_flag,
    opps_missing_link.dim_crm_opportunity_id,
    opps_missing_link.sdr_bdr_user_id,
    opps_missing_link.net_arr,
    opps_missing_link.net_arr_stage_1,
    opps_missing_link.xdr_net_arr_stage_1,
    opps_missing_link.xdr_net_arr_stage_3,
    opps_missing_link.sales_accepted_date,
    opps_missing_link.sales_accepted_fiscal_quarter_name,
    opps_missing_link.sao_day_of_fiscal_quarter,
    opps_missing_link.stage_0_pending_acceptance_date,
    opps_missing_link.stage_0_pending_acceptance_month,
    opps_missing_link.stage_0_pending_acceptance_fiscal_quarter_name,
    opps_missing_link.stage_1_discovery_date,
    opps_missing_link.stage_1_discovery_month,
    opps_missing_link.stage_1_discovery_fiscal_quarter_name,
    opps_missing_link.stage_2_scoping_date,
    opps_missing_link.stage_2_scoping_month,
    opps_missing_link.stage_2_scoping_fiscal_quarter_name,
    opps_missing_link.stage_3_technical_evaluation_date,
    opps_missing_link.stage_3_technical_evaluation_month,
    opps_missing_link.stage_3_technical_evaluation_fiscal_quarter_name, 
    opps_missing_link.days_in_1_discovery,
    opps_missing_link.days_in_sao,
    opps_missing_link.days_since_last_activity,
    opps_missing_link.sales_qualified_source_name,
    opps_missing_link.report_opportunity_user_segment,
    opps_missing_link.report_opportunity_user_geo,
    opps_missing_link.report_opportunity_user_region,
    opps_missing_link.report_opportunity_user_area,
    opps_missing_link.report_user_segment_geo_region_area,
    opps_missing_link.parent_crm_account_territory,
    opps_missing_link.parent_crm_account_sales_segment,
    opps_missing_link.parent_crm_account_geo,
    opps_missing_link.parent_crm_account_region,
    opps_missing_link.parent_crm_account_area,
    opps_missing_link.deal_path_name,
    opps_missing_link.opp_created_date,
    opps_missing_link.close_date,
    opps_missing_link.pipeline_created_date,
    opps_missing_link.activity_to_sao_days,
    opps_missing_link.order_type,
    opps_missing_link.stage_name,
    opps_missing_link.product_category,
    opps_missing_link.product_details,
    opps_missing_link.products_purchased,
    opps_missing_link.crm_account_focus_account,
    opps_missing_link.sales_dev_bdr_or_sdr,
    opps_missing_link.opportunity_sales_development_representative,
    opps_missing_link.opportunity_business_development_representative,
    opps_missing_link.crm_opp_owner_sales_segment_stamped,
    opps_missing_link.crm_opp_owner_business_unit_stamped,
    opps_missing_link.crm_opp_owner_geo_stamped,
    opps_missing_link.crm_opp_owner_region_stamped,
    opps_missing_link.crm_opp_owner_area_stamped,
    opps_missing_link.is_sao,
    opps_missing_link.is_net_arr_closed_deal,
    opps_missing_link.is_net_arr_pipeline_created,
    opps_missing_link.is_eligible_age_analysis,
    opps_missing_link.is_eligible_open_pipeline,
    sales_dev_hierarchy.*
  FROM opps_missing_link
  LEFT JOIN sales_dev_hierarchy 
    ON opps_missing_link.sdr_bdr_user_id = sales_dev_hierarchy.sales_dev_rep_user_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-09-06",
    updated_date="2024-05-07",
  ) }}