{{ config(
  materialized='table'
  ) }}

WITH target_geos AS (
  -- This table is the target subset of the assigned user geo values in Salesforce
  SELECT *
  FROM (
    VALUES
    ('AMER'),
    ('EMEA'),
    ('APJ'),
    ('APAC'),
    ('JAPAN'),
    ('PUBSEC'),
    ('SMB'),
    ('CHANNEL')
  ) AS geos (target_geos)
),

target_user_roles AS (
  -- This table is a list of the user roles and PUBSEC access of those roles
  -- for those Salesforce users that do not have an assigned geo
  SELECT *
  FROM (
    VALUES
    ('CMO', TRUE),
    ('CRO', TRUE),
    ('Director - SA High Velocity', TRUE),
    ('Director CSM', TRUE),
    ('Director of Professional Services', TRUE),
    ('Executive', TRUE),
    ('Field CTO', TRUE),
    ('Field Marketing Director', TRUE),
    ('Field Marketing Manager - Global', TRUE),
    ('Global CSE Director', TRUE),
    ('VP Sales Development', TRUE),
    ('VPSA', TRUE),
    ('Executive - Global Minus Pubsec', FALSE),
    ('Implementation Engineers - Global (with/without Pubsec)', FALSE),
    ('Marketing Operations Manager', FALSE),
    ('Marketing Program Manager', FALSE)
  ) AS roles (target_roles, has_pubsec)
),

sfdc_user_source AS (
  SELECT *
  FROM {{ ref('dim_crm_user') }}
),

tableau_group_source AS (
  SELECT *
  FROM {{ ref('tableau_cloud_groups_source') }}
),

sfdc_filtered AS (
  -- This CTE is for applying the user parameters that will later be used for application of geos.
  SELECT
    sfdc_user_source.dim_crm_user_id,
    sfdc_user_source.user_email,
    sfdc_user_source.crm_user_geo,
    sfdc_user_source.user_role_name,
    IFF(target_geos.target_geos IS NOT NULL, TRUE, FALSE)        AS has_user_geo,
    IFF(target_user_roles.target_roles IS NOT NULL, TRUE, FALSE) AS has_global,
    IFF(
      target_user_roles.target_roles IS NOT NULL
      AND target_user_roles.has_pubsec = TRUE, TRUE, FALSE
    )                                                            AS has_pubsec
  FROM sfdc_user_source
  LEFT JOIN target_geos
    ON LOWER(sfdc_user_source.crm_user_geo) = LOWER(target_geos.target_geos)
  LEFT JOIN target_user_roles
    ON LOWER(sfdc_user_source.user_role_name) = LOWER(target_user_roles.target_roles)
  WHERE sfdc_user_source.is_active = TRUE

),

tableau_safe_users AS (
  -- This CTE is for collecting a the list of SAFE Tableau users from the primary Tableau site.
  SELECT user_email
  FROM tableau_group_source
  WHERE site_luid = '2ae725b5-5a7b-40ba-a05c-35b7aa9ab731' -- Main Tableau Site
    AND group_name = 'General SAFE Access'
    AND group_luid = 'e926984d-06de-4536-b572-6d09075a21be' -- General SAFE Access
),

geo_list AS (
  SELECT DISTINCT crm_user_geo
  FROM sfdc_filtered
  WHERE crm_user_geo IS NOT NULL
    AND has_user_geo = TRUE
),

geo_users AS (
  -- This CTE if for filtering to a list of Salesforce users that have an assigned geo
  SELECT
    dim_crm_user_id,
    crm_user_geo AS crm_geo,
    user_email,
    'sfdc_user_geo' AS entitlement_basis
  FROM sfdc_filtered
  WHERE has_user_geo
),

all_accounts AS (
  -- This CTE if for filtering to a list of Salesforce users that have no assigned geo but 
  -- have SAFE access and one of the target Salesforce user roles
  SELECT
    geo_list.crm_user_geo AS crm_geo,
    sfdc_filtered.user_email,
    'tableau_safe_and_sfdc_role' AS entitlement_basis
  FROM sfdc_filtered
  INNER JOIN tableau_safe_users
    ON sfdc_filtered.user_email = tableau_safe_users.user_email
  LEFT JOIN geo_users
    ON sfdc_filtered.dim_crm_user_id = geo_users.dim_crm_user_id
  CROSS JOIN geo_list
  WHERE geo_users.user_email IS NULL
    AND sfdc_filtered.has_global = TRUE
    AND sfdc_filtered.has_pubsec = TRUE
),

non_pubsec AS (
  -- This CTE if for filtering to a list of Salesforce users that have no assigned geo but 
  -- have SAFE access, one of the target Salesforce user roles, but do not ave access the PUBSEC geo
  SELECT
    geo_list.crm_user_geo AS crm_geo,
    sfdc_filtered.user_email,
    'tableau_safe_and_sfdc_role_less_pubsec' AS entitlement_basis
  FROM sfdc_filtered
  INNER JOIN tableau_safe_users
    ON sfdc_filtered.user_email = tableau_safe_users.user_email
  LEFT JOIN geo_users
    ON sfdc_filtered.dim_crm_user_id = geo_users.dim_crm_user_id
  CROSS JOIN geo_list
  WHERE geo_users.user_email IS NULL
    AND sfdc_filtered.has_pubsec = FALSE
    AND sfdc_filtered.has_global = TRUE
    AND LOWER(geo_list.crm_user_geo) != LOWER('PUBSEC')
),

channel_sfdc_users AS (
  SELECT
    sfdc_user_source.user_email,
    target_geos.target_geos AS crm_geo,
    'all_sfdc_users' AS entitlement_basis
  FROM sfdc_user_source
  LEFT JOIN target_geos
    ON target_geos.target_geos = 'CHANNEL'
  WHERE sfdc_user_source.is_active = TRUE 
),

combined AS (
  SELECT 
    crm_geo,
    user_email,
    entitlement_basis
  FROM non_pubsec

  UNION ALL

  SELECT 
    crm_geo,
    user_email,
    entitlement_basis
  FROM all_accounts

  UNION ALL

  SELECT DISTINCT
    geo_users.crm_geo,
    geo_users.user_email,
    entitlement_basis
  FROM geo_users

  UNION ALL

  SELECT DISTINCT
    channel_sfdc_users.crm_geo,
    channel_sfdc_users.user_email,
    entitlement_basis
  FROM channel_sfdc_users
)

SELECT *
FROM combined
