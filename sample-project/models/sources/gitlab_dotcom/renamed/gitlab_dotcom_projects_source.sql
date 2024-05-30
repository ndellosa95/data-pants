    
WITH all_projects AS (

  SELECT 
    id::NUMBER                                                                  AS project_id,   
    description::VARCHAR                                                        AS project_description,
    import_source::VARCHAR                                                      AS project_import_source,
    issues_template::VARCHAR                                                    AS project_issues_template,
    name::VARCHAR                                                               AS project_name,
    path::VARCHAR                                                               AS project_path,
    import_url::VARCHAR                                                         AS project_import_url,
    merge_requests_template                                                     AS project_merge_requests_template,
    created_at::TIMESTAMP                                                       AS created_at,
    updated_at::TIMESTAMP                                                       AS updated_at,
    creator_id::NUMBER                                                          AS creator_id,
    namespace_id::NUMBER                                                        AS namespace_id,
    last_activity_at::TIMESTAMP                                                 AS last_activity_at,
    {{ visibility_level_name('visibility_level') }}                             AS visibility_level,
    archived::BOOLEAN                                                           AS archived,
    IFF(avatar IS NULL, FALSE, TRUE)::BOOLEAN                                   AS has_avatar,
    star_count::NUMBER                                                          AS project_star_count,
    merge_requests_rebase_enabled::BOOLEAN                                      AS merge_requests_rebase_enabled,
    IFF(LOWER(import_type) = 'nan', NULL, import_type)                          AS import_type,
    approvals_before_merge::NUMBER                                              AS approvals_before_merge,
    reset_approvals_on_push::BOOLEAN                                            AS reset_approvals_on_push,
    merge_requests_ff_only_enabled::BOOLEAN                                     AS merge_requests_ff_only_enabled,
    mirror::BOOLEAN                                                             AS mirror,
    mirror_user_id::NUMBER                                                      AS mirror_user_id,
    shared_runners_enabled::BOOLEAN                                             AS shared_runners_enabled,
    build_allow_git_fetch::BOOLEAN                                              AS build_allow_git_fetch,
    build_timeout::NUMBER                                                       AS build_timeout,
    mirror_trigger_builds::BOOLEAN                                              AS mirror_trigger_builds,
    pending_delete::BOOLEAN                                                     AS pending_delete,
    public_builds::BOOLEAN                                                      AS public_builds,
    last_repository_check_failed::BOOLEAN                                       AS last_repository_check_failed,
    last_repository_check_at::TIMESTAMP                                         AS last_repository_check_at,
    container_registry_enabled::BOOLEAN                                         AS container_registry_enabled,
    only_allow_merge_if_pipeline_succeeds::BOOLEAN                              AS only_allow_merge_if_pipeline_succeeds,
    has_external_issue_tracker::BOOLEAN                                         AS has_external_issue_tracker,
    repository_storage,
    repository_read_only::BOOLEAN                                               AS repository_read_only,
    request_access_enabled::BOOLEAN                                             AS request_access_enabled,
    has_external_wiki::BOOLEAN                                                  AS has_external_wiki,
    ci_config_path,
    lfs_enabled::BOOLEAN                                                        AS lfs_enabled,
    only_allow_merge_if_all_discussions_are_resolved::BOOLEAN                   AS only_allow_merge_if_all_discussions_are_resolved,
    repository_size_limit::NUMBER                                               AS repository_size_limit,
    printing_merge_request_link_enabled::BOOLEAN                                AS printing_merge_request_link_enabled,
    IFF(auto_cancel_pending_pipelines :: int = 1, TRUE, FALSE)                  AS has_auto_canceling_pending_pipelines,
    service_desk_enabled::BOOLEAN                                               AS service_desk_enabled,
    IFF(LOWER(delete_error) = 'nan', NULL, delete_error)                        AS delete_error,
    last_repository_updated_at::TIMESTAMP                                       AS last_repository_updated_at,
    storage_version::NUMBER                                                     AS storage_version,
    resolve_outdated_diff_discussions::BOOLEAN                                  AS resolve_outdated_diff_discussions,
    disable_overriding_approvers_per_merge_request::BOOLEAN                     AS disable_overriding_approvers_per_merge_request,
    remote_mirror_available_overridden::BOOLEAN                                 AS remote_mirror_available_overridden,
    only_mirror_protected_branches::BOOLEAN                                     AS only_mirror_protected_branches,
    pull_mirror_available_overridden::BOOLEAN                                   AS pull_mirror_available_overridden,
    mirror_overwrites_diverged_branches::BOOLEAN                                AS mirror_overwrites_diverged_branches,
    external_authorization_classification_label,
    project_namespace_id::NUMBER AS project_namespace_id
  FROM {{ ref('gitlab_dotcom_projects_dedupe_source') }}
  
), 

internal_projects AS (

  SELECT 
    id::NUMBER                                                                  AS internal_project_id,
    description::VARCHAR                                                        AS internal_project_description,   
    name::VARCHAR                                                               AS internal_project_name,
    path::VARCHAR                                                               AS internal_project_path,
    import_url::VARCHAR                                                         AS internal_project_import_url,
    created_at::TIMESTAMP                                                       AS internal_created_at,
    updated_at::TIMESTAMP                                                       AS internal_updated_at,
    namespace_id::NUMBER                                                        AS internal_namespace_id
  FROM {{ ref('gitlab_dotcom_projects_internal_only_dedupe_source') }}
  
), 

combined AS (

  SELECT 
    all_projects.project_id                                                     AS project_id,
    internal_projects.internal_project_description                              AS project_description ,
    all_projects.project_import_source                                          AS project_import_source,
    all_projects.project_issues_template                                        AS project_issues_template,
    internal_projects.internal_project_name                                     AS project_name,
    internal_projects.internal_project_path                                     AS project_path,
    internal_projects.internal_project_import_url                               AS project_import_url,
    all_projects.project_merge_requests_template                                AS project_merge_requests_template,
    all_projects.created_at                                                     AS created_at,
    all_projects.updated_at                                                     AS updated_at,
    all_projects.creator_id                                                     AS creator_id,
    all_projects.namespace_id                                                   AS namespace_id,
    all_projects.last_activity_at                                               AS last_activity_at,
    all_projects.visibility_level                                               AS visibility_level,
    all_projects.archived                                                       AS archived,
    all_projects.has_avatar                                                     AS has_avatar,
    all_projects.project_star_count                                             AS project_star_count,
    all_projects.merge_requests_rebase_enabled                                  AS merge_requests_rebase_enabled,
    all_projects.import_type                                                    AS import_type,
    all_projects.approvals_before_merge                                         AS approvals_before_merge,
    all_projects.reset_approvals_on_push                                        AS reset_approvals_on_push,
    all_projects.merge_requests_ff_only_enabled                                 AS merge_requests_ff_only_enabled,
    all_projects.mirror                                                         AS mirror,
    all_projects.mirror_user_id                                                 AS mirror_user_id,
    all_projects.shared_runners_enabled                                         AS shared_runners_enabled,
    all_projects.build_allow_git_fetch                                          AS build_allow_git_fetch,
    all_projects.build_timeout                                                  AS build_timeout,
    all_projects.mirror_trigger_builds                                          AS mirror_trigger_builds,
    all_projects.pending_delete                                                 AS pending_delete,
    all_projects.public_builds                                                  AS public_builds,
    all_projects.last_repository_check_failed                                   AS last_repository_check_failed,
    all_projects.last_repository_check_at                                       AS last_repository_check_at,
    all_projects.container_registry_enabled                                     AS container_registry_enabled,
    all_projects.only_allow_merge_if_pipeline_succeeds                          AS only_allow_merge_if_pipeline_succeeds,
    all_projects.has_external_issue_tracker                                     AS has_external_issue_tracker,
    all_projects.repository_storage                                             AS repository_storage,
    all_projects.repository_read_only                                           AS repository_read_only,
    all_projects.request_access_enabled                                         AS request_access_enabled,
    all_projects.has_external_wiki                                              AS has_external_wiki,
    all_projects.ci_config_path                                                 AS ci_config_path,
    all_projects.lfs_enabled                                                    AS lfs_enabled,
    all_projects.only_allow_merge_if_all_discussions_are_resolved               AS only_allow_merge_if_all_discussions_are_resolved,
    all_projects.repository_size_limit                                          AS repository_size_limit,
    all_projects.printing_merge_request_link_enabled                            AS printing_merge_request_link_enabled,
    all_projects.has_auto_canceling_pending_pipelines                           AS has_auto_canceling_pending_pipelines,
    all_projects.service_desk_enabled                                           AS service_desk_enabled,
    all_projects.delete_error                                                   AS delete_error,
    all_projects.last_repository_updated_at                                     AS last_repository_updated_at,
    all_projects.storage_version                                                AS storage_version,
    all_projects.resolve_outdated_diff_discussions                              AS resolve_outdated_diff_discussions,
    all_projects.disable_overriding_approvers_per_merge_request                 AS disable_overriding_approvers_per_merge_request,
    all_projects.remote_mirror_available_overridden                             AS remote_mirror_available_overridden,
    all_projects.only_mirror_protected_branches                                 AS only_mirror_protected_branches,
    all_projects.pull_mirror_available_overridden                               AS pull_mirror_available_overridden,
    all_projects.mirror_overwrites_diverged_branches                            AS mirror_overwrites_diverged_branches,
    all_projects.external_authorization_classification_label                    AS external_authorization_classification_label,
    all_projects.project_namespace_id                                           AS project_namespace_id
  FROM all_projects
  LEFT JOIN internal_projects
    ON all_projects.project_id = internal_projects.internal_project_id
    
)

SELECT *
FROM combined