{{
  config(
    materialized='table',
    tags=["mnpi_exception"],
  )
}}

{{ simple_cte([
    ('monthly_saas_metrics','fct_saas_product_usage_metrics_monthly'),
    ('monthly_sm_metrics','fct_ping_instance_metric_wave_monthly'),
    ('billing_accounts','dim_billing_account'),
    ('location_country', 'dim_location_country'),
    ('subscriptions', 'dim_subscription'),
    ('namespaces', 'dim_namespace'),
    ('charges', 'mart_charge'),
    ('dates', 'dim_date'),
    ('aggregated_metrics', 'redis_namespace_snowplow_clicks_aggregated_workspace'),
    ('redis_metrics_28d_user', 'rpt_user_based_metric_counts_namespace_monthly'),
    ('redis_metrics_all_time_event', 'rpt_event_based_metric_counts_namespace_all_time'),
    ('dim_product_detail', 'dim_product_detail')
]) }}


, most_recent_subscription_version AS (
    SELECT
      subscription_name,
      subscription_status,
      subscription_start_date,
      subscription_end_date,
      ROW_NUMBER() OVER(
        PARTITION BY
          subscription_name
        ORDER BY
          subscription_version DESC
      )
    FROM subscriptions
    WHERE subscription_status IN (
      'Active',
      'Cancelled'
    )
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY
        subscription_name
      ORDER BY
        subscription_version DESC
    ) = 1
    
), subscription_with_deployment_type AS (
  
    SELECT DISTINCT
        charges.dim_subscription_id,
        dim_product_detail.product_delivery_type,
        dim_product_detail.product_deployment_type
    FROM charges
    LEFT JOIN dim_product_detail
      ON charges.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    WHERE dim_product_detail.product_deployment_type IN ('Self-Managed', 'Dedicated')

), zuora_licenses_per_subscription AS (
  
    SELECT
      dates.first_day_of_month AS month,
      subscriptions.dim_subscription_id_original,
      SUM(charges.quantity) AS license_user_count
    FROM charges
    JOIN dates ON charges.effective_start_month <= dates.date_actual
      AND (charges.effective_end_month > dates.date_actual
       OR charges.effective_end_month IS NULL)
      AND dates.day_of_month = 1
    LEFT JOIN subscriptions ON charges.dim_subscription_id = subscriptions.dim_subscription_id
    WHERE charges.subscription_status IN ('Active','Cancelled')
      AND charges.product_tier_name != 'Storage'
      AND charges.rate_plan_charge_name NOT IN (
        'Dedicated - Administration Fee [Large] - 1 Year',
        'Dedicated - Administration Fee  [XLarge] - 1 Year',
        'Dedicated - Administration Fee [2XLarge] - 1 Year',
        'Dedicated - Storage 10GB - 1 Year'
      )
    {{ dbt_utils.group_by(n = 2) }}
    
), action_active_users_project_repo_users AS (
  
    SELECT
      *
    FROM aggregated_metrics 
    WHERE event_action = 'action_active_users_project_repo'
  
), p_terraform_state_api_unique_users AS (
  
    SELECT
      *
    FROM aggregated_metrics 
    WHERE event_action = 'p_terraform_state_api_unique_users'
   
), user_packages AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.user_packages.user_packages_total_unique_counts_monthly'

), project_repo AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'usage_activity_by_stage_monthly.create.action_monthly_active_users_project_repo'

), analytics_valuestream AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.analytics.g_analytics_valuestream_monthly'

), packages_pushed AS (

    SELECT
      *
    FROM redis_metrics_all_time_event
    WHERE metrics_path = 'counts.package_events_i_package_push_package_by_deploy_token'

), packages_pulled AS (

    SELECT
      *
    FROM redis_metrics_all_time_event
    WHERE metrics_path = 'counts.package_events_i_package_pull_package_by_guest'

), single_file_edit AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.ide_edit.g_edit_by_sfe_monthly'

), mrs_created AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.code_review.i_code_review_user_create_mr_monthly'

), pipelines_devops AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.ci_templates.p_ci_templates_implicit_auto_devops_monthly'

), ide_edit AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'usage_activity_by_stage_monthly.create.action_monthly_active_users_ide_edit'

), user_approve_mr AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.code_review.i_code_review_user_approve_mr_monthly'

), audit_users AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'counts_monthly.aggregated_metrics.compliance_features_track_unique_visits_union'

), epics_users AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.epics_usage.epics_usage_total_unique_counts_monthly'

), iterations_users AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.issues_edit.g_project_management_issue_iteration_changed_monthly'

), issues_edit_users AS (

    SELECT
      *
    FROM redis_metrics_28d_user
    WHERE metrics_path = 'redis_hll_counters.issues_edit.issues_edit_total_unique_counts_monthly'

), sm_paid_user_metrics AS (

    SELECT
      monthly_sm_metrics.snapshot_month,
      monthly_sm_metrics.dim_subscription_id,
      NULL                                                                         AS dim_namespace_id,
      NULL                                                                         AS namespace_name,
      NULL                                                                         AS namespace_creation_date,
      monthly_sm_metrics.dim_instance_id                                           AS uuid,
      monthly_sm_metrics.hostname,
      monthly_sm_metrics.dim_installation_id,
      {{ get_keyed_nulls('billing_accounts.dim_billing_account_id') }}              AS dim_billing_account_id,
      {{ get_keyed_nulls('billing_accounts.dim_crm_account_id') }}                  AS dim_crm_account_id,
      monthly_sm_metrics.dim_subscription_id_original,
      subscriptions.subscription_name,
      subscriptions.subscription_status,
      most_recent_subscription_version.subscription_status AS subscription_status_most_recent_version,
      subscriptions.term_start_date,
      subscriptions.term_end_date,
      most_recent_subscription_version.subscription_start_date,
      most_recent_subscription_version.subscription_end_date,
      monthly_sm_metrics.snapshot_date_id,
      monthly_sm_metrics.ping_created_at,
      monthly_sm_metrics.dim_ping_instance_id                                       AS dim_usage_ping_id,
      monthly_sm_metrics.instance_type,
      monthly_sm_metrics.included_in_health_measures_str,
      monthly_sm_metrics.cleaned_version,
      location_country.country_name,
      location_country.iso_2_country_code,
      location_country.iso_3_country_code,
      COALESCE(monthly_sm_metrics.ping_delivery_type, subscription_with_deployment_type.product_delivery_type, 'Self-Managed')
                                                                                   AS delivery_type,
      COALESCE(monthly_sm_metrics.ping_deployment_type, subscription_with_deployment_type.product_deployment_type, 'Self-Managed')
                                                                                   AS deployment_type,
      monthly_sm_metrics.installation_creation_date,
      -- Wave 1
      DIV0(
        monthly_sm_metrics.billable_user_count, 
        COALESCE(
          zuora_licenses_per_subscription.license_user_count, 
          monthly_sm_metrics.license_user_count)
      )                                                                             AS license_utilization,
      monthly_sm_metrics.billable_user_count,
      monthly_sm_metrics.active_user_count,
      monthly_sm_metrics.max_historical_user_count,
      COALESCE(
        zuora_licenses_per_subscription.license_user_count, 
        monthly_sm_metrics.license_user_count)                                      AS license_user_count,
      IFF(
        zuora_licenses_per_subscription.license_user_count IS NOT NULL, 
        'Zuora',
        'Service Ping')                                                             AS license_user_count_source,
      -- Wave 2 & 3
      monthly_sm_metrics.umau_28_days_user,
      monthly_sm_metrics.action_monthly_active_users_project_repo_28_days_user,
      monthly_sm_metrics.merge_requests_28_days_user,
      monthly_sm_metrics.projects_with_repositories_enabled_28_days_user,
      monthly_sm_metrics.commit_comment_all_time_event,
      monthly_sm_metrics.source_code_pushes_all_time_event,
      monthly_sm_metrics.ci_pipelines_28_days_user,
      monthly_sm_metrics.ci_internal_pipelines_28_days_user,
      monthly_sm_metrics.ci_builds_28_days_user,
      monthly_sm_metrics.ci_builds_all_time_user,
      monthly_sm_metrics.ci_builds_all_time_event,
      monthly_sm_metrics.ci_runners_all_time_event,
      monthly_sm_metrics.auto_devops_enabled_all_time_event,
      monthly_sm_metrics.gitlab_shared_runners_enabled,
      monthly_sm_metrics.container_registry_enabled,
      monthly_sm_metrics.template_repositories_all_time_event,
      monthly_sm_metrics.ci_pipeline_config_repository_28_days_user,
      monthly_sm_metrics.user_unique_users_all_secure_scanners_28_days_user,
      monthly_sm_metrics.user_sast_jobs_28_days_user,
      monthly_sm_metrics.user_dast_jobs_28_days_user,
      monthly_sm_metrics.user_dependency_scanning_jobs_28_days_user,
      monthly_sm_metrics.user_license_management_jobs_28_days_user,
      monthly_sm_metrics.user_secret_detection_jobs_28_days_user,
      monthly_sm_metrics.user_container_scanning_jobs_28_days_user,
      monthly_sm_metrics.object_store_packages_enabled,
      monthly_sm_metrics.projects_with_packages_all_time_event,
      monthly_sm_metrics.projects_with_packages_28_days_event,
      monthly_sm_metrics.deployments_28_days_user,
      monthly_sm_metrics.releases_28_days_user,
      monthly_sm_metrics.epics_28_days_user,
      monthly_sm_metrics.issues_28_days_user,
      -- Wave 3.1
      monthly_sm_metrics.ci_internal_pipelines_all_time_event,
      monthly_sm_metrics.ci_external_pipelines_all_time_event,
      monthly_sm_metrics.merge_requests_all_time_event,
      monthly_sm_metrics.todos_all_time_event,
      monthly_sm_metrics.epics_all_time_event,
      monthly_sm_metrics.issues_all_time_event,
      monthly_sm_metrics.projects_all_time_event,
      monthly_sm_metrics.deployments_28_days_event,
      monthly_sm_metrics.packages_28_days_event,
      monthly_sm_metrics.dast_jobs_all_time_event,
      monthly_sm_metrics.license_management_jobs_all_time_event,
      monthly_sm_metrics.secret_detection_jobs_all_time_event,
      monthly_sm_metrics.projects_jenkins_active_all_time_event,
      monthly_sm_metrics.projects_bamboo_active_all_time_event,
      monthly_sm_metrics.projects_jira_active_all_time_event,
      monthly_sm_metrics.projects_drone_ci_active_all_time_event,
      monthly_sm_metrics.projects_github_active_all_time_event,
      monthly_sm_metrics.projects_jira_dvcs_cloud_active_all_time_event,
      monthly_sm_metrics.projects_with_repositories_enabled_all_time_event,
      monthly_sm_metrics.protected_branches_all_time_event,
      monthly_sm_metrics.remote_mirrors_all_time_event,
      monthly_sm_metrics.projects_enforcing_code_owner_approval_28_days_user,
      monthly_sm_metrics.project_clusters_enabled_28_days_user,
      monthly_sm_metrics.analytics_28_days_user,
      monthly_sm_metrics.issues_edit_28_days_user,
      monthly_sm_metrics.user_packages_28_days_user,
      monthly_sm_metrics.terraform_state_api_28_days_user,
      monthly_sm_metrics.incident_management_28_days_user,
      -- Wave 3.2
      monthly_sm_metrics.auto_devops_enabled,
      monthly_sm_metrics.gitaly_clusters_instance,
      monthly_sm_metrics.epics_deepest_relationship_level_instance,
      monthly_sm_metrics.network_policy_forwards_all_time_event,
      monthly_sm_metrics.network_policy_drops_all_time_event,
      monthly_sm_metrics.requirements_with_test_report_all_time_event,
      monthly_sm_metrics.requirement_test_reports_ci_all_time_event,
      monthly_sm_metrics.projects_imported_from_github_all_time_event,
      monthly_sm_metrics.projects_jira_dvcs_server_active_all_time_event,
      monthly_sm_metrics.service_desk_issues_all_time_event,
      monthly_sm_metrics.ci_pipelines_all_time_user,
      monthly_sm_metrics.service_desk_issues_28_days_user,
      monthly_sm_metrics.projects_jira_active_28_days_user,
      monthly_sm_metrics.projects_jira_dvcs_server_active_28_days_user,
      monthly_sm_metrics.merge_requests_with_required_code_owners_28_days_user,
      monthly_sm_metrics.analytics_value_stream_28_days_event,
      monthly_sm_metrics.code_review_user_approve_mr_28_days_user,
      monthly_sm_metrics.epics_usage_28_days_user,
      monthly_sm_metrics.project_management_issue_milestone_changed_28_days_user,
      monthly_sm_metrics.project_management_issue_iteration_changed_28_days_user,
      -- Wave 5.1
      monthly_sm_metrics.protected_branches_28_days_user,
      monthly_sm_metrics.ci_cd_lead_time_usage_28_days_event,
      monthly_sm_metrics.ci_cd_deployment_frequency_usage_28_days_event,
      monthly_sm_metrics.projects_with_repositories_enabled_all_time_user,
      monthly_sm_metrics.environments_all_time_event,
      monthly_sm_metrics.feature_flags_all_time_event,
      monthly_sm_metrics.successful_deployments_28_days_event,
      monthly_sm_metrics.failed_deployments_28_days_event,
      monthly_sm_metrics.projects_compliance_framework_all_time_event,
      monthly_sm_metrics.commit_ci_config_file_28_days_user,
      monthly_sm_metrics.view_audit_all_time_user,
      -- Wave 5.2
      monthly_sm_metrics.dependency_scanning_jobs_all_time_user,
      monthly_sm_metrics.analytics_devops_adoption_all_time_user,
      monthly_sm_metrics.projects_imported_all_time_event,
      monthly_sm_metrics.preferences_security_dashboard_28_days_user,
      monthly_sm_metrics.web_ide_edit_28_days_user,
      monthly_sm_metrics.auto_devops_pipelines_all_time_event,
      monthly_sm_metrics.projects_prometheus_active_all_time_event,
      monthly_sm_metrics.prometheus_enabled,
      monthly_sm_metrics.prometheus_metrics_enabled,
      monthly_sm_metrics.group_saml_enabled,
      monthly_sm_metrics.jira_issue_imports_all_time_event,
      monthly_sm_metrics.author_epic_all_time_user,
      monthly_sm_metrics.author_issue_all_time_user,
      monthly_sm_metrics.failed_deployments_28_days_user,
      monthly_sm_metrics.successful_deployments_28_days_user,
      -- Wave 5.3
      monthly_sm_metrics.geo_enabled,
      monthly_sm_metrics.auto_devops_pipelines_28_days_user,
      monthly_sm_metrics.active_instance_runners_all_time_event,
      monthly_sm_metrics.active_group_runners_all_time_event,
      monthly_sm_metrics.active_project_runners_all_time_event,
      monthly_sm_metrics.gitaly_version,
      monthly_sm_metrics.gitaly_servers_all_time_event,
      -- Wave 6.0
      monthly_sm_metrics.api_fuzzing_scans_all_time_event,
      monthly_sm_metrics.api_fuzzing_scans_28_days_event,
      monthly_sm_metrics.coverage_fuzzing_scans_all_time_event,
      monthly_sm_metrics.coverage_fuzzing_scans_28_days_event,
      monthly_sm_metrics.secret_detection_scans_all_time_event,
      monthly_sm_metrics.secret_detection_scans_28_days_event,
      monthly_sm_metrics.dependency_scanning_scans_all_time_event,
      monthly_sm_metrics.dependency_scanning_scans_28_days_event,
      monthly_sm_metrics.container_scanning_scans_all_time_event,
      monthly_sm_metrics.container_scanning_scans_28_days_event,
      monthly_sm_metrics.dast_scans_all_time_event,
      monthly_sm_metrics.dast_scans_28_days_event,
      monthly_sm_metrics.sast_scans_all_time_event,
      monthly_sm_metrics.sast_scans_28_days_event,
      -- Wave 6.1
      monthly_sm_metrics.packages_pushed_registry_all_time_event,
      monthly_sm_metrics.packages_pulled_registry_all_time_event,
      monthly_sm_metrics.compliance_dashboard_view_28_days_user,
      monthly_sm_metrics.audit_screen_view_28_days_user,
      monthly_sm_metrics.instance_audit_screen_view_28_days_user,
      monthly_sm_metrics.credential_inventory_view_28_days_user,
      monthly_sm_metrics.compliance_frameworks_pipeline_all_time_event,
      monthly_sm_metrics.compliance_frameworks_pipeline_28_days_event,
      monthly_sm_metrics.groups_streaming_destinations_all_time_event,
      monthly_sm_metrics.audit_event_destinations_all_time_event,
      monthly_sm_metrics.audit_event_destinations_28_days_event,
      monthly_sm_metrics.projects_status_checks_all_time_event,
      monthly_sm_metrics.external_status_checks_all_time_event,
      monthly_sm_metrics.paid_license_search_28_days_user,
      monthly_sm_metrics.last_activity_28_days_user,
      -- Wave 7
      monthly_sm_metrics.snippets_28_days_event,
      monthly_sm_metrics.single_file_editor_28_days_user,
      monthly_sm_metrics.merge_requests_created_28_days_event,
      monthly_sm_metrics.merge_requests_created_28_days_user,
      monthly_sm_metrics.merge_requests_approval_rules_28_days_event,
      monthly_sm_metrics.custom_compliance_frameworks_28_days_event,
      monthly_sm_metrics.projects_security_policy_28_days_event,
      monthly_sm_metrics.merge_requests_security_policy_28_days_user,
      monthly_sm_metrics.pipelines_implicit_auto_devops_28_days_event,
      monthly_sm_metrics.pipeline_schedules_28_days_user,
      -- Wave 8
      monthly_sm_metrics.ci_internal_pipelines_28_days_event,
      -- Wave 9
      monthly_sm_metrics.ci_builds_28_days_event,
      monthly_sm_metrics.audit_features_28_days_user,
      monthly_sm_metrics.groups_all_time_event,
      monthly_sm_metrics.commit_ci_config_file_7_days_user,
      monthly_sm_metrics.ci_pipeline_config_repository_all_time_user,
      monthly_sm_metrics.ci_pipeline_config_repository_all_time_event,
      monthly_sm_metrics.pipeline_schedules_all_time_event,
      monthly_sm_metrics.pipeline_schedules_all_time_user,
      -- Data Quality Flag
      monthly_sm_metrics.is_latest_data
    FROM monthly_sm_metrics
    LEFT JOIN billing_accounts
      ON monthly_sm_metrics.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN location_country
      ON monthly_sm_metrics.dim_location_country_id = location_country.dim_location_country_id
    LEFT JOIN subscriptions
      ON subscriptions.dim_subscription_id = monthly_sm_metrics.dim_subscription_id
    LEFT JOIN subscription_with_deployment_type
      ON subscription_with_deployment_type.dim_subscription_id = monthly_sm_metrics.dim_subscription_id
    LEFT JOIN most_recent_subscription_version
      ON subscriptions.subscription_name = most_recent_subscription_version.subscription_name
    LEFT JOIN zuora_licenses_per_subscription 
      ON zuora_licenses_per_subscription.dim_subscription_id_original = monthly_sm_metrics.dim_subscription_id_original
      AND zuora_licenses_per_subscription.month = monthly_sm_metrics.snapshot_month

), saas_paid_user_metrics AS (

    SELECT
      monthly_saas_metrics.snapshot_month,
      monthly_saas_metrics.dim_subscription_id,
      monthly_saas_metrics.dim_namespace_id::VARCHAR                                AS dim_namespace_id,
      namespaces.namespace_name,
      namespaces.created_at                                                         AS namespace_creation_date,
      NULL                                                                          AS uuid,
      NULL                                                                          AS hostname,
      NULL                                                                          AS dim_installation_id,
      {{ get_keyed_nulls('billing_accounts.dim_billing_account_id') }}              AS dim_billing_account_id,
      {{ get_keyed_nulls('billing_accounts.dim_crm_account_id') }}                      AS dim_crm_account_id,
      monthly_saas_metrics.dim_subscription_id_original,
      subscriptions.subscription_name,
      subscriptions.subscription_status,
      most_recent_subscription_version.subscription_status AS subscription_status_most_recent_version,
      subscriptions.term_start_date,
      subscriptions.term_end_date,
      most_recent_subscription_version.subscription_start_date,
      most_recent_subscription_version.subscription_end_date,
      monthly_saas_metrics.snapshot_date_id,
      monthly_saas_metrics.ping_created_at,
      NULL                                                                          AS dim_usage_ping_id,
      monthly_saas_metrics.instance_type                                            AS instance_type,
      monthly_saas_metrics.included_in_health_measures_str                          AS included_in_health_measures_str,
      NULL                                                                          AS cleaned_version,
      NULL                                                                          AS country_name,
      NULL                                                                          AS iso_2_country_code,
      NULL                                                                          AS iso_3_country_code,
      'SaaS'                                                                        AS delivery_type,
      'GitLab.com'                                                                  AS deployment_type,
      NULL                                                                          AS installation_creation_date,
      -- Wave 1
      DIV0(
        monthly_saas_metrics.billable_user_count, 
        COALESCE(
          zuora_licenses_per_subscription.license_user_count,
          monthly_saas_metrics.subscription_seats)
      )                                                                             AS license_utilization,
      monthly_saas_metrics.billable_user_count,
      NULL                                                                          AS active_user_count,
      monthly_saas_metrics.max_historical_user_count,
      COALESCE(
        zuora_licenses_per_subscription.license_user_count,
        monthly_saas_metrics.subscription_seats)                                    AS license_user_count,
      IFF(
        zuora_licenses_per_subscription.license_user_count IS NOT NULL,
        'Zuora',
        'gitlabdotcom')                                                             AS license_user_count_source,
      -- Wave 2 & 3
      monthly_saas_metrics.umau_28_days_user,
      COALESCE(project_repo.distinct_users_whole_month, action_active_users_project_repo_users.distinct_users, 0)            AS action_monthly_active_users_project_repo_28_days_user,
      monthly_saas_metrics.merge_requests_28_days_user,
      monthly_saas_metrics.projects_with_repositories_enabled_28_days_user,
      monthly_saas_metrics.commit_comment_all_time_event,
      monthly_saas_metrics.source_code_pushes_all_time_event,
      monthly_saas_metrics.ci_pipelines_28_days_user,
      monthly_saas_metrics.ci_internal_pipelines_28_days_user,
      monthly_saas_metrics.ci_builds_28_days_user,
      monthly_saas_metrics.ci_builds_all_time_user,
      monthly_saas_metrics.ci_builds_all_time_event,
      monthly_saas_metrics.ci_runners_all_time_event,
      monthly_saas_metrics.auto_devops_enabled_all_time_event,
      monthly_saas_metrics.gitlab_shared_runners_enabled,
      monthly_saas_metrics.container_registry_enabled,
      monthly_saas_metrics.template_repositories_all_time_event,
      monthly_saas_metrics.ci_pipeline_config_repository_28_days_user,
      monthly_saas_metrics.user_unique_users_all_secure_scanners_28_days_user,
      monthly_saas_metrics.user_sast_jobs_28_days_user,
      monthly_saas_metrics.user_dast_jobs_28_days_user,
      monthly_saas_metrics.user_dependency_scanning_jobs_28_days_user,
      monthly_saas_metrics.user_license_management_jobs_28_days_user,
      monthly_saas_metrics.user_secret_detection_jobs_28_days_user,
      monthly_saas_metrics.user_container_scanning_jobs_28_days_user,
      monthly_saas_metrics.object_store_packages_enabled,
      monthly_saas_metrics.projects_with_packages_all_time_event,
      monthly_saas_metrics.projects_with_packages_28_days_event,
      monthly_saas_metrics.deployments_28_days_user,
      monthly_saas_metrics.releases_28_days_user,
      monthly_saas_metrics.epics_28_days_user,
      monthly_saas_metrics.issues_28_days_user,
      -- Wave 3.1
      monthly_saas_metrics.ci_internal_pipelines_all_time_event,
      monthly_saas_metrics.ci_external_pipelines_all_time_event,
      monthly_saas_metrics.merge_requests_all_time_event,
      monthly_saas_metrics.todos_all_time_event,
      monthly_saas_metrics.epics_all_time_event,
      monthly_saas_metrics.issues_all_time_event,
      monthly_saas_metrics.projects_all_time_event,
      monthly_saas_metrics.deployments_28_days_event,
      monthly_saas_metrics.packages_28_days_event,
      monthly_saas_metrics.dast_jobs_all_time_event,
      monthly_saas_metrics.license_management_jobs_all_time_event,
      monthly_saas_metrics.secret_detection_jobs_all_time_event,
      monthly_saas_metrics.projects_jenkins_active_all_time_event,
      monthly_saas_metrics.projects_bamboo_active_all_time_event,
      monthly_saas_metrics.projects_jira_active_all_time_event,
      monthly_saas_metrics.projects_drone_ci_active_all_time_event,
      monthly_saas_metrics.projects_github_active_all_time_event,
      monthly_saas_metrics.projects_jira_dvcs_cloud_active_all_time_event,
      monthly_saas_metrics.projects_with_repositories_enabled_all_time_event,
      monthly_saas_metrics.protected_branches_all_time_event,
      monthly_saas_metrics.remote_mirrors_all_time_event,
      monthly_saas_metrics.projects_enforcing_code_owner_approval_28_days_user,
      monthly_saas_metrics.project_clusters_enabled_28_days_user,
      monthly_saas_metrics.analytics_28_days_user,
      COALESCE(issues_edit_users.distinct_users_whole_month, 0) AS issues_edit_28_days_user,
      COALESCE(user_packages.distinct_users_whole_month, 0) AS user_packages_28_days_user,
      COALESCE(p_terraform_state_api_unique_users.distinct_users, 0) AS terraform_state_api_28_days_user,
      monthly_saas_metrics.incident_management_28_days_user,
      -- Wave 3.2
      monthly_saas_metrics.auto_devops_enabled,
      monthly_saas_metrics.gitaly_clusters_instance,
      monthly_saas_metrics.epics_deepest_relationship_level_instance,
      monthly_saas_metrics.network_policy_forwards_all_time_event,
      monthly_saas_metrics.network_policy_drops_all_time_event,
      monthly_saas_metrics.requirements_with_test_report_all_time_event,
      monthly_saas_metrics.requirement_test_reports_ci_all_time_event,
      monthly_saas_metrics.projects_imported_from_github_all_time_event,
      monthly_saas_metrics.projects_jira_dvcs_server_active_all_time_event,
      monthly_saas_metrics.service_desk_issues_all_time_event,
      monthly_saas_metrics.ci_pipelines_all_time_user,
      monthly_saas_metrics.service_desk_issues_28_days_user,
      monthly_saas_metrics.projects_jira_active_28_days_user,
      monthly_saas_metrics.projects_jira_dvcs_server_active_28_days_user,
      monthly_saas_metrics.merge_requests_with_required_code_owners_28_days_user,
      COALESCE(analytics_valuestream.distinct_users_whole_month, 0) AS analytics_value_stream_28_days_event,
      COALESCE(user_approve_mr.distinct_users_whole_month, 0) AS code_review_user_approve_mr_28_days_user,
      COALESCE(epics_users.distinct_users_whole_month, 0) AS epics_usage_28_days_user,
      monthly_saas_metrics.project_management_issue_milestone_changed_28_days_user,
      COALESCE(iterations_users.distinct_users_whole_month, 0) AS project_management_issue_iteration_changed_28_days_user,
      -- Wave 5.1
      monthly_saas_metrics.protected_branches_28_days_user,
      monthly_saas_metrics.ci_cd_lead_time_usage_28_days_event,
      monthly_saas_metrics.ci_cd_deployment_frequency_usage_28_days_event,
      monthly_saas_metrics.projects_with_repositories_enabled_all_time_user,
      monthly_saas_metrics.environments_all_time_event,
      monthly_saas_metrics.feature_flags_all_time_event,
      monthly_saas_metrics.successful_deployments_28_days_event,
      monthly_saas_metrics.failed_deployments_28_days_event,
      monthly_saas_metrics.projects_compliance_framework_all_time_event,
      monthly_saas_metrics.commit_ci_config_file_28_days_user,
      monthly_saas_metrics.view_audit_all_time_user,
      -- Wave 5.2
      monthly_saas_metrics.dependency_scanning_jobs_all_time_user,
      monthly_saas_metrics.analytics_devops_adoption_all_time_user,
      monthly_saas_metrics.projects_imported_all_time_event,
      monthly_saas_metrics.preferences_security_dashboard_28_days_user,
      COALESCE(ide_edit.distinct_users_whole_month, 0) AS web_ide_edit_28_days_user,
      monthly_saas_metrics.auto_devops_pipelines_all_time_event,
      monthly_saas_metrics.projects_prometheus_active_all_time_event,
      monthly_saas_metrics.prometheus_enabled,
      monthly_saas_metrics.prometheus_metrics_enabled,
      monthly_saas_metrics.group_saml_enabled,
      monthly_saas_metrics.jira_issue_imports_all_time_event,
      monthly_saas_metrics.author_epic_all_time_user,
      monthly_saas_metrics.author_issue_all_time_user,
      monthly_saas_metrics.failed_deployments_28_days_user,
      monthly_saas_metrics.successful_deployments_28_days_user,
      -- Wave 5.3
      monthly_saas_metrics.geo_enabled,
      monthly_saas_metrics.auto_devops_pipelines_28_days_user,
      monthly_saas_metrics.active_instance_runners_all_time_event,
      monthly_saas_metrics.active_group_runners_all_time_event,
      monthly_saas_metrics.active_project_runners_all_time_event,
      monthly_saas_metrics.gitaly_version,
      monthly_saas_metrics.gitaly_servers_all_time_event,
      -- Wave 6.0
      monthly_saas_metrics.api_fuzzing_scans_all_time_event,
      monthly_saas_metrics.api_fuzzing_scans_28_days_event,
      monthly_saas_metrics.coverage_fuzzing_scans_all_time_event,
      monthly_saas_metrics.coverage_fuzzing_scans_28_days_event,
      monthly_saas_metrics.secret_detection_scans_all_time_event,
      monthly_saas_metrics.secret_detection_scans_28_days_event,
      monthly_saas_metrics.dependency_scanning_scans_all_time_event,
      monthly_saas_metrics.dependency_scanning_scans_28_days_event,
      monthly_saas_metrics.container_scanning_scans_all_time_event,
      monthly_saas_metrics.container_scanning_scans_28_days_event,
      monthly_saas_metrics.dast_scans_all_time_event,
      monthly_saas_metrics.dast_scans_28_days_event,
      monthly_saas_metrics.sast_scans_all_time_event,
      monthly_saas_metrics.sast_scans_28_days_event,
      -- Wave 6.1
      COALESCE(packages_pushed.monthly_value, 0) AS packages_pushed_registry_all_time_event,
      COALESCE(packages_pulled.monthly_value, 0) AS packages_pulled_registry_all_time_event,
      monthly_saas_metrics.compliance_dashboard_view_28_days_user,
      monthly_saas_metrics.audit_screen_view_28_days_user,
      monthly_saas_metrics.instance_audit_screen_view_28_days_user,
      monthly_saas_metrics.credential_inventory_view_28_days_user,
      monthly_saas_metrics.compliance_frameworks_pipeline_all_time_event,
      monthly_saas_metrics.compliance_frameworks_pipeline_28_days_event,
      monthly_saas_metrics.groups_streaming_destinations_all_time_event,
      monthly_saas_metrics.audit_event_destinations_all_time_event,
      monthly_saas_metrics.audit_event_destinations_28_days_event,
      monthly_saas_metrics.projects_status_checks_all_time_event,
      monthly_saas_metrics.external_status_checks_all_time_event,
      monthly_saas_metrics.paid_license_search_28_days_user,
      monthly_saas_metrics.last_activity_28_days_user,
      -- Wave 7
      monthly_saas_metrics.snippets_28_days_event,
      COALESCE(single_file_edit.distinct_users_whole_month, 0) AS single_file_editor_28_days_user,
      monthly_saas_metrics.merge_requests_created_28_days_event,
      COALESCE(mrs_created.distinct_users_whole_month, 0) AS merge_requests_created_28_days_user,
      monthly_saas_metrics.merge_requests_approval_rules_28_days_event,
      monthly_saas_metrics.custom_compliance_frameworks_28_days_event,
      monthly_saas_metrics.projects_security_policy_28_days_event,
      monthly_saas_metrics.merge_requests_security_policy_28_days_user,
      COALESCE(pipelines_devops.distinct_users_whole_month, 0) AS pipelines_implicit_auto_devops_28_days_event,
      monthly_saas_metrics.pipeline_schedules_28_days_user,
      -- Wave 8
      monthly_saas_metrics.ci_internal_pipelines_28_days_event,
      --Wave 9
      monthly_saas_metrics.ci_builds_28_days_event,
      COALESCE(audit_users.distinct_users_whole_month, 0) AS audit_features_28_days_user,
      monthly_saas_metrics.groups_all_time_event,
      monthly_saas_metrics.commit_ci_config_file_7_days_user,
      monthly_saas_metrics.ci_pipeline_config_repository_all_time_user,
      monthly_saas_metrics.ci_pipeline_config_repository_all_time_event,
      monthly_saas_metrics.pipeline_schedules_all_time_event,
      monthly_saas_metrics.pipeline_schedules_all_time_user,
      -- Data Quality Flag
      monthly_saas_metrics.is_latest_data
    FROM monthly_saas_metrics
    LEFT JOIN billing_accounts
      ON monthly_saas_metrics.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN subscriptions
      ON subscriptions.dim_subscription_id = monthly_saas_metrics.dim_subscription_id
    LEFT JOIN most_recent_subscription_version
      ON subscriptions.subscription_name = most_recent_subscription_version.subscription_name
    LEFT JOIN zuora_licenses_per_subscription 
      ON zuora_licenses_per_subscription.dim_subscription_id_original = monthly_saas_metrics.dim_subscription_id_original
      AND zuora_licenses_per_subscription.month = monthly_saas_metrics.snapshot_month
    LEFT JOIN namespaces 
      ON namespaces.dim_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN action_active_users_project_repo_users
      ON action_active_users_project_repo_users.date_month = monthly_saas_metrics.snapshot_month 
      AND action_active_users_project_repo_users.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN p_terraform_state_api_unique_users
      ON p_terraform_state_api_unique_users.date_month = monthly_saas_metrics.snapshot_month 
      AND p_terraform_state_api_unique_users.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN user_packages
      ON user_packages.date_month = monthly_saas_metrics.snapshot_month
      AND user_packages.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN project_repo
      ON project_repo.date_month = monthly_saas_metrics.snapshot_month
      AND project_repo.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN analytics_valuestream
      ON analytics_valuestream.date_month = monthly_saas_metrics.snapshot_month
      AND analytics_valuestream.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN packages_pushed
      ON packages_pushed.month = monthly_saas_metrics.snapshot_month
      AND packages_pushed.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN packages_pulled
      ON packages_pulled.month = monthly_saas_metrics.snapshot_month
      AND packages_pulled.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN single_file_edit
      ON single_file_edit.date_month = monthly_saas_metrics.snapshot_month
      AND single_file_edit.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN mrs_created
      ON mrs_created.date_month = monthly_saas_metrics.snapshot_month
      AND mrs_created.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN pipelines_devops
      ON pipelines_devops.date_month = monthly_saas_metrics.snapshot_month
      AND pipelines_devops.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN ide_edit
      ON ide_edit.date_month = monthly_saas_metrics.snapshot_month
      AND ide_edit.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN user_approve_mr
      ON user_approve_mr.date_month = monthly_saas_metrics.snapshot_month
      AND user_approve_mr.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN audit_users
      ON audit_users.date_month = monthly_saas_metrics.snapshot_month
      AND audit_users.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN epics_users
      ON epics_users.date_month = monthly_saas_metrics.snapshot_month
      AND epics_users.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN iterations_users
      ON iterations_users.date_month = monthly_saas_metrics.snapshot_month
      AND iterations_users.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id
    LEFT JOIN issues_edit_users
      ON issues_edit_users.date_month = monthly_saas_metrics.snapshot_month
      AND issues_edit_users.ultimate_parent_namespace_id = monthly_saas_metrics.dim_namespace_id

), unioned AS (

    SELECT *
    FROM sm_paid_user_metrics

    UNION ALL

    SELECT *
    FROM saas_paid_user_metrics

), final AS (
  
    SELECT
      unioned.*,
      {{ dbt_utils.generate_surrogate_key(
        [
          'snapshot_month',
          'dim_subscription_id',
          'deployment_type',
          'uuid',
          'hostname',
          'dim_namespace_id'
        ]
      ) }} AS primary_key
    FROM unioned
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@utkarsh060",
    created_date="2022-01-14",
    updated_date="2024-03-13"
) }}
