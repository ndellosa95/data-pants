{{ simple_cte([
    ('fct_crm_opportunity','wk_fct_crm_opportunity_7th_day_weekly_snapshot'),
    ('dim_crm_account','dim_crm_account_daily_snapshot'),
    ('dim_crm_user', 'wk_prep_crm_user_daily_snapshot'),
    ('dim_date', 'dim_date'),
    ('dim_crm_user_hierarchy', 'wk_dim_crm_user_hierarchy'),
]) }},


final AS (


  SELECT

    fct_crm_opportunity.crm_opportunity_snapshot_id,
    fct_crm_opportunity.dim_crm_opportunity_id,
    fct_crm_opportunity.dim_crm_user_id,
    fct_crm_opportunity.snapshot_id,
    fct_crm_opportunity.dim_sales_qualified_source_id,
    fct_crm_opportunity.dim_order_type_id,
    fct_crm_opportunity.dim_order_type_live_id,
    
    fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,

    fct_crm_opportunity.crm_current_account_set_sales_segment_live AS crm_current_account_set_sales_segment,
    fct_crm_opportunity.crm_current_account_set_geo_live AS crm_current_account_set_geo,
    fct_crm_opportunity.crm_current_account_set_region_live AS crm_current_account_set_region,
    fct_crm_opportunity.crm_current_account_set_area_live AS crm_current_account_set_area,
    fct_crm_opportunity.crm_current_account_set_business_unit_live AS crm_current_account_set_business_unit,
    fct_crm_opportunity.crm_current_account_set_role_name,
    fct_crm_opportunity.crm_current_account_set_role_level_1,
    fct_crm_opportunity.crm_current_account_set_role_level_2,
    fct_crm_opportunity.crm_current_account_set_role_level_3,
    fct_crm_opportunity.crm_current_account_set_role_level_4,
    fct_crm_opportunity.crm_current_account_set_role_level_5,

    fct_crm_opportunity.merged_crm_opportunity_id,
    fct_crm_opportunity.dim_crm_account_id,
    fct_crm_opportunity.dim_crm_person_id,
    fct_crm_opportunity.sfdc_contact_id,
    fct_crm_opportunity.record_type_id,
    fct_crm_opportunity.opportunity_name,
    fct_crm_opportunity.report_user_segment_geo_region_area_sqs_ot,
    fct_crm_opportunity.opp_owner_name,
    fct_crm_opportunity.sales_qualified_source_name,
    fct_crm_opportunity.sales_qualified_source_grouped,
    fct_crm_opportunity.order_type,
    fct_crm_opportunity.order_type_live,
    fct_crm_opportunity.order_type_grouped,
    fct_crm_opportunity.stage_name,
    fct_crm_opportunity.deal_path_name,
    fct_crm_opportunity.sales_type,
    fct_crm_opportunity.snapshot_date,
    fct_crm_opportunity.snapshot_month,
    fct_crm_opportunity.snapshot_fiscal_year,
    fct_crm_opportunity.snapshot_fiscal_quarter_name,
    fct_crm_opportunity.snapshot_fiscal_quarter_date,
    fct_crm_opportunity.snapshot_day_of_fiscal_quarter_normalised,
    fct_crm_opportunity.snapshot_day_of_fiscal_year_normalised,

    
    fct_crm_opportunity.days_in_0_pending_acceptance,
    fct_crm_opportunity.days_in_1_discovery,
    fct_crm_opportunity.days_in_2_scoping,
    fct_crm_opportunity.days_in_3_technical_evaluation,
    fct_crm_opportunity.days_in_4_proposal,
    fct_crm_opportunity.days_in_5_negotiating,
    fct_crm_opportunity.ssp_id,
    fct_crm_opportunity.ga_client_id,
    fct_crm_opportunity.is_closed,
    fct_crm_opportunity.is_won,
    fct_crm_opportunity.is_refund,
    fct_crm_opportunity.is_downgrade,
    fct_crm_opportunity.is_swing_deal,
    fct_crm_opportunity.is_edu_oss,
    fct_crm_opportunity.is_web_portal_purchase,
    fct_crm_opportunity.fpa_master_bookings_flag,
    fct_crm_opportunity.is_sao,
    fct_crm_opportunity.is_sdr_sao,
    fct_crm_opportunity.is_net_arr_closed_deal,
    fct_crm_opportunity.is_new_logo_first_order,
    fct_crm_opportunity.is_net_arr_pipeline_created_combined,
    fct_crm_opportunity.is_win_rate_calc,
    fct_crm_opportunity.is_closed_won,
    fct_crm_opportunity.is_stage_1_plus,
    fct_crm_opportunity.is_stage_3_plus,
    fct_crm_opportunity.is_stage_4_plus,
    fct_crm_opportunity.is_lost,
    fct_crm_opportunity.is_open,
    fct_crm_opportunity.is_active,
    fct_crm_opportunity.is_credit,
    fct_crm_opportunity.is_renewal,
    fct_crm_opportunity.is_deleted,
    fct_crm_opportunity.is_excluded_from_pipeline_created_combined,
    fct_crm_opportunity.created_in_snapshot_quarter_deal_count,
    fct_crm_opportunity.is_duplicate,
    fct_crm_opportunity.is_contract_reset,
    fct_crm_opportunity.is_comp_new_logo_override,
    fct_crm_opportunity.is_eligible_open_pipeline_combined,
    fct_crm_opportunity.is_eligible_age_analysis_combined,
    fct_crm_opportunity.is_eligible_churn_contraction,
    fct_crm_opportunity.is_booked_net_arr,
    fct_crm_opportunity.is_abm_tier_sao,
    fct_crm_opportunity.is_abm_tier_closed_won,
    fct_crm_opportunity.primary_solution_architect,
    fct_crm_opportunity.product_details,
    fct_crm_opportunity.product_category,
    fct_crm_opportunity.intended_product_tier,
    fct_crm_opportunity.products_purchased,
    fct_crm_opportunity.growth_type,
    fct_crm_opportunity.opportunity_deal_size,
    fct_crm_opportunity.closed_buckets,
    fct_crm_opportunity.calculated_deal_size,
    fct_crm_opportunity.deal_size,
    fct_crm_opportunity.lead_source,
    fct_crm_opportunity.dr_partner_deal_type,
    fct_crm_opportunity.dr_partner_engagement,
    fct_crm_opportunity.partner_account,
    fct_crm_opportunity.dr_status,
    fct_crm_opportunity.dr_deal_id,
    fct_crm_opportunity.dr_primary_registration,
    fct_crm_opportunity.distributor,
    fct_crm_opportunity.influence_partner,
    fct_crm_opportunity.fulfillment_partner,
    fct_crm_opportunity.platform_partner,
    fct_crm_opportunity.partner_track,
    fct_crm_opportunity.resale_partner_track,
    fct_crm_opportunity.is_public_sector_opp,
    fct_crm_opportunity.is_registration_from_portal,
    fct_crm_opportunity.calculated_discount,
    fct_crm_opportunity.partner_discount,
    fct_crm_opportunity.partner_discount_calc,
    fct_crm_opportunity.comp_channel_neutral,

    dim_crm_account.dim_parent_crm_account_id,

    -- account fields
    dim_crm_account.crm_account_name,
    dim_crm_account.parent_crm_account_name,
    dim_crm_account.parent_crm_account_business_unit,
    dim_crm_account.parent_crm_account_sales_segment,
    dim_crm_account.parent_crm_account_geo,
    dim_crm_account.parent_crm_account_region,
    dim_crm_account.parent_crm_account_area,
    dim_crm_account.parent_crm_account_territory,
    dim_crm_account.parent_crm_account_role_type,
    dim_crm_account.parent_crm_account_max_family_employee,
    dim_crm_account.parent_crm_account_upa_country,
    dim_crm_account.parent_crm_account_upa_state,
    dim_crm_account.parent_crm_account_upa_city,
    dim_crm_account.parent_crm_account_upa_street,
    dim_crm_account.parent_crm_account_upa_postal_code,
    dim_crm_account.crm_account_employee_count,
    dim_crm_account.crm_account_gtm_strategy,
    dim_crm_account.crm_account_focus_account,
    dim_crm_account.crm_account_zi_technologies,
    dim_crm_account.is_jihu_account,

    -- fields to be removed after a bug is fixed in Tableau
    NULL AS sao_crm_opp_owner_sales_segment_stamped,
    NULL AS sao_crm_opp_owner_sales_segment_stamped_grouped,
    NULL AS sao_crm_opp_owner_geo_stamped,
    NULL AS sao_crm_opp_owner_region_stamped,
    NULL AS sao_crm_opp_owner_area_stamped,
    NULL AS sao_crm_opp_owner_segment_region_stamped_grouped,
    NULL AS sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
    NULL AS crm_opp_owner_stamped_name,
    NULL AS crm_account_owner_stamped_name,
    NULL AS crm_opp_owner_sales_segment_stamped,
    NULL AS crm_opp_owner_sales_segment_stamped_grouped,
    NULL AS crm_opp_owner_geo_stamped,
    NULL AS crm_opp_owner_region_stamped,
    NULL AS crm_opp_owner_area_stamped,
    NULL AS crm_opp_owner_business_unit_stamped,
    NULL AS crm_opp_owner_sales_segment_region_stamped_grouped,
    NULL AS crm_opp_owner_sales_segment_geo_region_area_stamped,
    NULL AS crm_opp_owner_user_role_type_stamped,
    NULL AS crm_user_sales_segment,
    NULL AS crm_user_geo,
    NULL AS crm_user_region,
    NULL AS crm_user_area,
    NULL AS crm_user_business_unit,
    NULL AS crm_user_sales_segment_grouped,
    NULL AS crm_user_sales_segment_region_grouped,
    NULL AS crm_user_role_name,
    NULL AS crm_user_role_level_1,
    NULL AS crm_user_role_level_2,
    NULL AS crm_user_role_level_3,
    NULL AS crm_user_role_level_4,
    NULL AS crm_user_role_level_5,
    NULL AS crm_account_user_sales_segment,
    NULL AS crm_account_user_sales_segment_grouped,
    NULL AS crm_account_user_geo,
    NULL AS crm_account_user_region,
    NULL AS crm_account_user_area,
    NULL AS crm_account_user_sales_segment_region_grouped,

    -- channel fields
    
    partner_account.crm_account_name AS partner_account_name,
    partner_account.gitlab_partner_program  AS partner_gitlab_program,
    fulfillment_partner.crm_account_name AS fulfillment_partner_name,

    -- Dates
    DAYNAME(dim_date.current_date_actual) AS current_day_name,  --need to add this field to date_details
    dim_date.current_date_actual,
    dim_date.current_fiscal_year,
    dim_date.current_first_day_of_fiscal_year,
    dim_date.current_fiscal_quarter_name_fy,
    dim_date.current_first_day_of_month,
    dim_date.current_first_day_of_fiscal_quarter,
    dim_date.current_day_of_month,
    dim_date.current_day_of_fiscal_quarter,
    dim_date.current_day_of_fiscal_year,
    CASE WHEN current_day_name = 'Sun' THEN dim_date.current_date_actual
      ELSE DATEADD('day', -1, DATE_TRUNC('week', dim_date.current_date_actual)) END     
                                                                    AS current_first_day_of_week,
    FLOOR((DATEDIFF(day, dim_date.current_first_day_of_fiscal_quarter, dim_date.current_date_actual) / 7))                   
                                                                    AS current_week_of_fiscal_quarter_normalised,
    FLOOR((DATEDIFF(day, dim_date.current_first_day_of_fiscal_quarter, dim_date.current_date_actual) / 7)) 
                                                                    AS current_week_of_fiscal_quarter,
    created_date.date_actual                                        AS created_date,
    created_date.first_day_of_month                                 AS created_month,
    created_date.first_day_of_fiscal_quarter                        AS created_fiscal_quarter_date,
    created_date.fiscal_quarter_name_fy                             AS created_fiscal_quarter_name,
    created_date.fiscal_year                                        AS created_fiscal_year,
    sales_accepted_date.date_actual                                 AS sales_accepted_date,
    sales_accepted_date.first_day_of_month                          AS sales_accepted_month,
    sales_accepted_date.first_day_of_fiscal_quarter                 AS sales_accepted_fiscal_quarter_date,
    sales_accepted_date.fiscal_quarter_name_fy                      AS sales_accepted_fiscal_quarter_name,
    sales_accepted_date.fiscal_year                                 AS sales_accepted_fiscal_year,
    close_date.date_actual                                          AS close_date,
    close_date.first_day_of_month                                   AS close_month,
    close_date.first_day_of_fiscal_quarter                          AS close_fiscal_quarter_date,
    close_date.fiscal_quarter_name_fy                               AS close_fiscal_quarter_name,
    close_date.fiscal_year                                          AS close_fiscal_year,
    stage_0_pending_acceptance_date.date_actual                     AS stage_0_pending_acceptance_date,
    stage_0_pending_acceptance_date.first_day_of_month              AS stage_0_pending_acceptance_month,
    stage_0_pending_acceptance_date.first_day_of_fiscal_quarter     AS stage_0_pending_acceptance_fiscal_quarter_date,
    stage_0_pending_acceptance_date.fiscal_quarter_name_fy          AS stage_0_pending_acceptance_fiscal_quarter_name,
    stage_0_pending_acceptance_date.fiscal_year                     AS stage_0_pending_acceptance_fiscal_year,
    stage_1_discovery_date.date_actual                              AS stage_1_discovery_date,
    stage_1_discovery_date.first_day_of_month                       AS stage_1_discovery_month,
    stage_1_discovery_date.first_day_of_fiscal_quarter              AS stage_1_discovery_fiscal_quarter_date,
    stage_1_discovery_date.fiscal_quarter_name_fy                   AS stage_1_discovery_fiscal_quarter_name,
    stage_1_discovery_date.fiscal_year                              AS stage_1_discovery_fiscal_year,
    stage_2_scoping_date.date_actual                                AS stage_2_scoping_date,
    stage_2_scoping_date.first_day_of_month                         AS stage_2_scoping_month,
    stage_2_scoping_date.first_day_of_fiscal_quarter                AS stage_2_scoping_fiscal_quarter_date,
    stage_2_scoping_date.fiscal_quarter_name_fy                     AS stage_2_scoping_fiscal_quarter_name,
    stage_2_scoping_date.fiscal_year                                AS stage_2_scoping_fiscal_year,
    stage_3_technical_evaluation_date.date_actual                   AS stage_3_technical_evaluation_date,
    stage_3_technical_evaluation_date.first_day_of_month            AS stage_3_technical_evaluation_month,
    stage_3_technical_evaluation_date.first_day_of_fiscal_quarter   AS stage_3_technical_evaluation_fiscal_quarter_date,
    stage_3_technical_evaluation_date.fiscal_quarter_name_fy        AS stage_3_technical_evaluation_fiscal_quarter_name,
    stage_3_technical_evaluation_date.fiscal_year                   AS stage_3_technical_evaluation_fiscal_year,
    stage_4_proposal_date.date_actual                               AS stage_4_proposal_date,
    stage_4_proposal_date.first_day_of_month                        AS stage_4_proposal_month,
    stage_4_proposal_date.first_day_of_fiscal_quarter               AS stage_4_proposal_fiscal_quarter_date,
    stage_4_proposal_date.fiscal_quarter_name_fy                    AS stage_4_proposal_fiscal_quarter_name,
    stage_4_proposal_date.fiscal_year                               AS stage_4_proposal_fiscal_year,
    stage_5_negotiating_date.date_actual                            AS stage_5_negotiating_date,
    stage_5_negotiating_date.first_day_of_month                     AS stage_5_negotiating_month,
    stage_5_negotiating_date.first_day_of_fiscal_quarter            AS stage_5_negotiating_fiscal_quarter_date,
    stage_5_negotiating_date.fiscal_quarter_name_fy                 AS stage_5_negotiating_fiscal_quarter_name,
    stage_5_negotiating_date.fiscal_year                            AS stage_5_negotiating_fiscal_year,
    stage_6_awaiting_signature_date.date_actual                     AS stage_6_awaiting_signature_date,
    stage_6_awaiting_signature_date.date_actual                     AS stage_6_awaiting_signature_date_date, -- added to maintain workspace model temporarily 
    stage_6_awaiting_signature_date.first_day_of_month              AS stage_6_awaiting_signature_date_month,
    stage_6_awaiting_signature_date.first_day_of_fiscal_quarter     AS stage_6_awaiting_signature_date_fiscal_quarter_date,
    stage_6_awaiting_signature_date.fiscal_quarter_name_fy          AS stage_6_awaiting_signature_date_fiscal_quarter_name,
    stage_6_awaiting_signature_date.fiscal_year                     AS stage_6_awaiting_signature_date_fiscal_year,
    stage_6_closed_won_date.date_actual                             AS stage_6_closed_won_date,
    stage_6_closed_won_date.first_day_of_month                      AS stage_6_closed_won_month,
    stage_6_closed_won_date.first_day_of_fiscal_quarter             AS stage_6_closed_won_fiscal_quarter_date,
    stage_6_closed_won_date.fiscal_quarter_name_fy                  AS stage_6_closed_won_fiscal_quarter_name,
    stage_6_closed_won_date.fiscal_year                             AS stage_6_closed_won_fiscal_year,
    stage_6_closed_lost_date.date_actual                            AS stage_6_closed_lost_date,
    stage_6_closed_lost_date.first_day_of_month                     AS stage_6_closed_lost_month,
    stage_6_closed_lost_date.first_day_of_fiscal_quarter            AS stage_6_closed_lost_fiscal_quarter_date,
    stage_6_closed_lost_date.fiscal_quarter_name_fy                 AS stage_6_closed_lost_fiscal_quarter_name,
    stage_6_closed_lost_date.fiscal_year                            AS stage_6_closed_lost_fiscal_year,
    subscription_start_date.date_actual                             AS subscription_start_date,
    subscription_start_date.first_day_of_month                      AS subscription_start_month,
    subscription_start_date.first_day_of_fiscal_quarter             AS subscription_start_fiscal_quarter_date,
    subscription_start_date.fiscal_quarter_name_fy                  AS subscription_start_fiscal_quarter_name,
    subscription_start_date.fiscal_year                             AS subscription_start_fiscal_year,
    subscription_end_date.date_actual                               AS subscription_end_date,
    subscription_end_date.first_day_of_month                        AS subscription_end_month,
    subscription_end_date.first_day_of_fiscal_quarter               AS subscription_end_fiscal_quarter_date,
    subscription_end_date.fiscal_quarter_name_fy                    AS subscription_end_fiscal_quarter_name,
    subscription_end_date.fiscal_year                               AS subscription_end_fiscal_year,
    sales_qualified_date.date_actual                                AS sales_qualified_date,
    sales_qualified_date.first_day_of_month                         AS sales_qualified_month,
    sales_qualified_date.first_day_of_fiscal_quarter                AS sales_qualified_fiscal_quarter_date,
    sales_qualified_date.fiscal_quarter_name_fy                     AS sales_qualified_fiscal_quarter_name,
    sales_qualified_date.fiscal_year                                AS sales_qualified_fiscal_year,
    last_activity_date.date_actual                                  AS last_activity_date,
    last_activity_date.first_day_of_month                           AS last_activity_month,
    last_activity_date.first_day_of_fiscal_quarter                  AS last_activity_fiscal_quarter_date,
    last_activity_date.fiscal_quarter_name_fy                       AS last_activity_fiscal_quarter_name,
    last_activity_date.fiscal_year                                  AS last_activity_fiscal_year,
    sales_last_activity_date.date_actual                            AS sales_last_activity_date,
    sales_last_activity_date.first_day_of_month                     AS sales_last_activity_month,
    sales_last_activity_date.first_day_of_fiscal_quarter            AS sales_last_activity_fiscal_quarter_date,
    sales_last_activity_date.fiscal_quarter_name_fy                 AS sales_last_activity_fiscal_quarter_name,
    sales_last_activity_date.fiscal_year                            AS sales_last_activity_fiscal_year,
    technical_evaluation_date.date_actual                           AS technical_evaluation_date,
    technical_evaluation_date.first_day_of_month                    AS technical_evaluation_month,
    technical_evaluation_date.first_day_of_fiscal_quarter           AS technical_evaluation_fiscal_quarter_date,
    technical_evaluation_date.fiscal_quarter_name_fy                AS technical_evaluation_fiscal_quarter_name,
    technical_evaluation_date.fiscal_year                           AS technical_evaluation_fiscal_year,
    arr_created_date.date_actual                                    AS arr_created_date,
    arr_created_date.first_day_of_month                             AS arr_created_month,
    arr_created_date.first_day_of_fiscal_quarter                    AS arr_created_fiscal_quarter_date,
    arr_created_date.fiscal_quarter_name_fy                         AS arr_created_fiscal_quarter_name,
    arr_created_date.fiscal_year                                    AS arr_created_fiscal_year,
    arr_created_date.date_actual                                    AS pipeline_created_date,
    arr_created_date.first_day_of_month                             AS pipeline_created_month,
    arr_created_date.first_day_of_fiscal_quarter                    AS pipeline_created_fiscal_quarter_date,
    arr_created_date.fiscal_quarter_name_fy                         AS pipeline_created_fiscal_quarter_name,
    arr_created_date.fiscal_year                                    AS pipeline_created_fiscal_year,
    arr_created_date.date_actual                                    AS net_arr_created_date,
    arr_created_date.first_day_of_month                             AS net_arr_created_month,
    arr_created_date.first_day_of_fiscal_quarter                    AS net_arr_created_fiscal_quarter_date,
    arr_created_date.fiscal_quarter_name_fy                         AS net_arr_created_fiscal_quarter_name,
    arr_created_date.fiscal_year                                    AS net_arr_created_fiscal_year,
    dim_date.date_day                                               AS snapshot_day,
    dim_date.day_name                                               AS snapshot_day_name, 
    dim_date.day_of_week                                            AS snapshot_day_of_week,
    dim_date.first_day_of_week                                      AS snapshot_first_day_of_week,
    dim_date.week_of_year                                           AS snapshot_week_of_year,
    dim_date.day_of_month                                           AS snapshot_day_of_month,
    dim_date.day_of_quarter                                         AS snapshot_day_of_quarter,
    dim_date.day_of_year                                            AS snapshot_day_of_year,
    dim_date.fiscal_quarter                                         AS snapshot_fiscal_quarter,
    dim_date.day_of_fiscal_quarter                                  AS snapshot_day_of_fiscal_quarter,
    dim_date.day_of_fiscal_year                                     AS snapshot_day_of_fiscal_year,
    dim_date.month_name                                             AS snapshot_month_name,
    dim_date.first_day_of_month                                     AS snapshot_first_day_of_month,
    dim_date.last_day_of_month                                      AS snapshot_last_day_of_month,
    dim_date.first_day_of_year                                      AS snapshot_first_day_of_year,
    dim_date.last_day_of_year                                       AS snapshot_last_day_of_year,
    dim_date.first_day_of_quarter                                   AS snapshot_first_day_of_quarter,
    dim_date.last_day_of_quarter                                    AS snapshot_last_day_of_quarter,
    dim_date.first_day_of_fiscal_quarter                            AS snapshot_first_day_of_fiscal_quarter,
    dim_date.last_day_of_fiscal_quarter                             AS snapshot_last_day_of_fiscal_quarter,
    dim_date.first_day_of_fiscal_year                               AS snapshot_first_day_of_fiscal_year,
    dim_date.last_day_of_fiscal_year                                AS snapshot_last_day_of_fiscal_year,
    dim_date.week_of_fiscal_year                                    AS snapshot_week_of_fiscal_year,
    dim_date.month_of_fiscal_year                                   AS snapshot_month_of_fiscal_year,
    dim_date.last_day_of_week                                       AS snapshot_last_day_of_week,
    dim_date.quarter_name                                           AS snapshot_quarter_name,
    dim_date.fiscal_quarter_name_fy                                 AS snapshot_fiscal_quarter_name_fy,
    dim_date.fiscal_quarter_number_absolute                         AS snapshot_fiscal_quarter_number_absolute,
    dim_date.fiscal_month_name                                      AS snapshot_fiscal_month_name,
    dim_date.fiscal_month_name_fy                                   AS snapshot_fiscal_month_name_fy,
    dim_date.holiday_desc                                           AS snapshot_holiday_desc,
    dim_date.is_holiday                                             AS snapshot_is_holiday,
    dim_date.last_month_of_fiscal_quarter                           AS snapshot_last_month_of_fiscal_quarter,
    dim_date.is_first_day_of_last_month_of_fiscal_quarter           AS snapshot_is_first_day_of_last_month_of_fiscal_quarter,
    dim_date.last_month_of_fiscal_year                              AS snapshot_last_month_of_fiscal_year,
    dim_date.is_first_day_of_last_month_of_fiscal_year              AS snapshot_is_first_day_of_last_month_of_fiscal_year,
    dim_date.days_in_month_count                                    AS snapshot_days_in_month_count,
    dim_date.week_of_month_normalised                               AS snapshot_week_of_month_normalised,
    dim_date.week_of_fiscal_quarter_normalised                      AS snapshot_week_of_fiscal_quarter_normalised,
    dim_date.is_first_day_of_fiscal_quarter_week                    AS snapshot_is_first_day_of_fiscal_quarter_week,
    dim_date.days_until_last_day_of_month                           AS snapshot_days_until_last_day_of_month,
    FLOOR((DATEDIFF(day, dim_date.first_day_of_fiscal_quarter, fct_crm_opportunity.snapshot_date) / 7)) 
                                                                    AS snapshot_week_of_fiscal_quarter,

    --additive fields
    fct_crm_opportunity.positive_booked_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.positive_booked_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.positive_open_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.positive_open_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.closed_deals_in_snapshot_quarter,
    fct_crm_opportunity.closed_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_1plus_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_3plus_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_4plus_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.open_1plus_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.open_3plus_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.open_4plus_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.created_arr_in_snapshot_quarter,
    fct_crm_opportunity.closed_won_opps_in_snapshot_quarter,
    fct_crm_opportunity.closed_opps_in_snapshot_quarter,
    fct_crm_opportunity.booked_net_arr_in_snapshot_quarter,
    fct_crm_opportunity.created_deals_in_snapshot_quarter,
    fct_crm_opportunity.cycle_time_in_days_in_snapshot_quarter,
    fct_crm_opportunity.booked_deal_count_in_snapshot_quarter,
    fct_crm_opportunity.created_arr,
    fct_crm_opportunity.closed_won_opps,
    fct_crm_opportunity.closed_opps,
    fct_crm_opportunity.closed_net_arr,
    fct_crm_opportunity.segment_order_type_iacv_to_net_arr_ratio,
    fct_crm_opportunity.calculated_from_ratio_net_arr,
    fct_crm_opportunity.net_arr,
    fct_crm_opportunity.raw_net_arr,
    fct_crm_opportunity.created_and_won_same_quarter_net_arr_combined,
    fct_crm_opportunity.new_logo_count,
    fct_crm_opportunity.amount,
    fct_crm_opportunity.recurring_amount,
    fct_crm_opportunity.true_up_amount,
    fct_crm_opportunity.proserv_amount,
    fct_crm_opportunity.other_non_recurring_amount,
    fct_crm_opportunity.arr_basis,
    fct_crm_opportunity.arr,
    fct_crm_opportunity.count_crm_attribution_touchpoints,
    fct_crm_opportunity.weighted_linear_iacv,
    fct_crm_opportunity.count_campaigns,
    fct_crm_opportunity.probability,
    fct_crm_opportunity.days_in_sao,
    fct_crm_opportunity.open_1plus_deal_count,
    fct_crm_opportunity.open_3plus_deal_count,
    fct_crm_opportunity.open_4plus_deal_count,
    fct_crm_opportunity.booked_deal_count,
    fct_crm_opportunity.churned_contraction_deal_count,
    fct_crm_opportunity.open_1plus_net_arr,
    fct_crm_opportunity.open_3plus_net_arr,
    fct_crm_opportunity.open_4plus_net_arr,
    fct_crm_opportunity.booked_net_arr,
    fct_crm_opportunity.churned_contraction_net_arr,
    fct_crm_opportunity.calculated_deal_count,
    fct_crm_opportunity.booked_churned_contraction_deal_count,
    fct_crm_opportunity.booked_churned_contraction_net_arr,
    fct_crm_opportunity.renewal_amount,
    fct_crm_opportunity.total_contract_value,
    fct_crm_opportunity.days_in_stage,
    fct_crm_opportunity.calculated_age_in_days,
    fct_crm_opportunity.days_since_last_activity,
    fct_crm_opportunity.pre_military_invasion_arr,
    fct_crm_opportunity.won_arr_basis_for_clari,
    fct_crm_opportunity.arr_basis_for_clari,
    fct_crm_opportunity.forecasted_churn_for_clari,
    fct_crm_opportunity.override_arr_basis_clari,
    fct_crm_opportunity.vsa_start_date_net_arr,
    fct_crm_opportunity.day_of_week,
    fct_crm_opportunity.first_day_of_week,
    fct_crm_opportunity.date_id,
    fct_crm_opportunity.fiscal_month_name_fy,
    fct_crm_opportunity.fiscal_quarter_name_fy,
    fct_crm_opportunity.first_day_of_fiscal_quarter,
    fct_crm_opportunity.first_day_of_fiscal_year,
    fct_crm_opportunity.last_day_of_week,
    fct_crm_opportunity.last_day_of_month,
    fct_crm_opportunity.last_day_of_fiscal_quarter,
    fct_crm_opportunity.last_day_of_fiscal_year,
    IFF(dim_date.current_first_day_of_fiscal_quarter = snapshot_first_day_of_fiscal_quarter, TRUE, FALSE) AS is_current_snapshot_quarter,
    IFF(current_first_day_of_week = dim_date.first_day_of_week, TRUE, FALSE) AS is_current_snapshot_week,
    'granular' AS source
  FROM fct_crm_opportunity
  LEFT JOIN dim_crm_account
    ON fct_crm_opportunity.dim_crm_account_id = dim_crm_account.dim_crm_account_id
      AND fct_crm_opportunity.snapshot_id = dim_crm_account.snapshot_id
  LEFT JOIN dim_crm_user AS opp_owner_live
    ON fct_crm_opportunity.dim_crm_user_id = opp_owner_live.dim_crm_user_id
      AND fct_crm_opportunity.snapshot_id = opp_owner_live.snapshot_id
  LEFT JOIN dim_crm_user AS account_owner_live
    ON dim_crm_account.dim_crm_user_id = account_owner_live.dim_crm_user_id
      AND dim_crm_account.snapshot_id = account_owner_live.snapshot_id
  LEFT JOIN dim_date 
    ON fct_crm_opportunity.snapshot_date = dim_date.date_actual
  LEFT JOIN dim_date created_date
    ON fct_crm_opportunity.created_date_id = created_date.date_id
  LEFT JOIN dim_date sales_accepted_date
    ON fct_crm_opportunity.sales_accepted_date_id = sales_accepted_date.date_id
  LEFT JOIN dim_date close_date
    ON fct_crm_opportunity.close_date_id = close_date.date_id
  LEFT JOIN dim_date stage_0_pending_acceptance_date
    ON fct_crm_opportunity.stage_0_pending_acceptance_date_id = stage_0_pending_acceptance_date.date_id
  LEFT JOIN dim_date stage_1_discovery_date
    ON fct_crm_opportunity.stage_1_discovery_date_id = stage_1_discovery_date.date_id
  LEFT JOIN dim_date stage_2_scoping_date
    ON fct_crm_opportunity.stage_2_scoping_date_id = stage_2_scoping_date.date_id
  LEFT JOIN dim_date stage_3_technical_evaluation_date
    ON fct_crm_opportunity.stage_3_technical_evaluation_date_id = stage_3_technical_evaluation_date.date_id
  LEFT JOIN dim_date stage_4_proposal_date
    ON fct_crm_opportunity.stage_4_proposal_date_id = stage_4_proposal_date.date_id
  LEFT JOIN dim_date stage_5_negotiating_date
    ON fct_crm_opportunity.stage_5_negotiating_date_id = stage_5_negotiating_date.date_id
  LEFT JOIN dim_date stage_6_awaiting_signature_date
      ON fct_crm_opportunity.stage_6_awaiting_signature_date_id = stage_6_awaiting_signature_date.date_id
  LEFT JOIN dim_date stage_6_closed_won_date
    ON fct_crm_opportunity.stage_6_closed_won_date_id = stage_6_closed_won_date.date_id
  LEFT JOIN dim_date stage_6_closed_lost_date
    ON fct_crm_opportunity.stage_6_closed_lost_date_id = stage_6_closed_lost_date.date_id
  LEFT JOIN dim_date subscription_start_date
    ON fct_crm_opportunity.subscription_start_date_id = subscription_start_date.date_id
  LEFT JOIN dim_date subscription_end_date
    ON fct_crm_opportunity.subscription_end_date_id = subscription_end_date.date_id
  LEFT JOIN dim_date sales_qualified_date
    ON fct_crm_opportunity.sales_qualified_date_id = sales_qualified_date.date_id
  LEFT JOIN dim_date last_activity_date
    ON fct_crm_opportunity.last_activity_date_id = last_activity_date.date_id
  LEFT JOIN dim_date sales_last_activity_date
    ON fct_crm_opportunity.sales_last_activity_date_id = sales_last_activity_date.date_id
  LEFT JOIN dim_date technical_evaluation_date
    ON fct_crm_opportunity.technical_evaluation_date_id = technical_evaluation_date.date_id
  LEFT JOIN dim_date arr_created_date 
    ON fct_crm_opportunity.arr_created_date_id = arr_created_date.date_id
  LEFT JOIN dim_crm_account AS partner_account
    ON fct_crm_opportunity.partner_account = partner_account.dim_crm_account_id
      AND fct_crm_opportunity.snapshot_id = partner_account.snapshot_id 
  LEFT JOIN dim_crm_account AS fulfillment_partner
    ON fct_crm_opportunity.fulfillment_partner = fulfillment_partner.dim_crm_account_id
      AND fct_crm_opportunity.snapshot_id = fulfillment_partner.snapshot_id
  LEFT JOIN dim_crm_user_hierarchy
    ON dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk = fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk
  


)

SELECT * 
FROM final