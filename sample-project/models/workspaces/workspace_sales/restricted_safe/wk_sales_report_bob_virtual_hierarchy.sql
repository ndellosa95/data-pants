{{ config(alias='report_bob_virtual_hierarchy') }}

WITH RECURSIVE date_details AS (

    SELECT *
    --FROM prod.workspace_sales.date_details
    FROM {{ ref('wk_sales_date_details') }}

 ), 
 sfdc_users_xf AS (

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

), 
raw_account AS (

    SELECT *
    FROM {{ source('salesforce', 'account') }}
    --FROM raw.salesforce_v2_stitch.account

 ), 
 mart_crm_account AS (

    SELECT acc.*

    --FROM prod.restricted_safe_common_mart_sales.mart_crm_account acc
    FROM {{ref('mart_crm_account')}} acc

 ), 
 mart_arr AS (

    SELECT *
    --FROM prod.restricted_safe_common_mart_sales.mart_arr
    FROM {{ref('mart_arr')}}

), 
arr_report_month AS (

    SELECT DISTINCT
        fiscal_year        AS report_fiscal_year,
        first_day_of_month AS report_month_date
    FROM date_details
    CROSS JOIN (SELECT DATEADD(MONTH, -1, DATEADD(day, -2, CURRENT_DATE)) AS today_date)
    WHERE date_actual = today_date

),
last_arr_report AS (

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
        raw_acc.lam_tier__c                             AS potential_lam_arr,

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
        raw_upa.account_demographic_max_family_employees__c AS account_demographics_upa_max_family_employees



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
),
upa_virtual_cte AS (

    SELECT
        upa_id                   AS real_upa_id,
        upa_name                 AS real_upa_name,
        upa_owner_id             AS real_upa_owner_id,
        upa_owner_geo            AS real_upa_owner_geo,
        account_id               AS virtual_upa_id,
        account_name             AS virtual_upa_name,
        account_owner_id         AS virtual_upa_owner_id,
        account_owner_geo   AS virtual_upa_owner_geo,
        account_id,
        account_name,
        account_owner_id,
        arr                      AS account_arr,
        1                        AS level
    FROM base_account
    -- account demographics geo of the child different than the one of the upa
    WHERE upa_owner_geo != account_owner_geo
    AND arr > 5000
    -- Do not allow pubsec accounts to be UPA heads
    --AND COALESCE(lower(pubsec_type),'') !='us-pubsec'
    UNION ALL
    SELECT
        upa.real_upa_id,
        upa.real_upa_name,
        upa.real_upa_owner_id,
        upa.real_upa_owner_geo,
        upa.virtual_upa_id,
        upa.virtual_upa_name,
        upa.virtual_upa_owner_id,
        upa.virtual_upa_owner_geo,
        child.account_id,
        child.account_name,
        child.account_owner_id,
        child.arr     AS account_arr,
        upa.level + 1 AS level
    FROM base_account AS child
    INNER JOIN upa_virtual_cte AS upa
        ON child.parent_id = upa.account_id

-- order the virtual upas paths by depth by geo
-- we select the longest path per virtual_upa_owner_geo and make it a virtual UPA
),
rank_virtual_upa_geo AS (

    SELECT
        real_upa_id,
        real_upa_name,
        virtual_upa_id,
        virtual_upa_owner_id,
        virtual_upa_owner_geo,
        virtual_upa_name,
        ROW_NUMBER() OVER (PARTITION BY real_upa_id, virtual_upa_owner_geo ORDER BY level DESC) AS rank_level
    FROM upa_virtual_cte
    QUALIFY rank_level = 1


-- assign the same virtual upa to all members of the hierarchy that share the same virtual geo
-- even if they are not within the same parent - child relationship
-- we adjust the previous hierarchy and flatten it per geo (based on the geo of the parent of the hierarchy)
-- some accounts within the hierarchy of the virtual account might be owned by reps outside of that geo
-- that will be caught through another flag
),
adjusted_virtual_hierarchy AS (

    SELECT DISTINCT
        head_of_geo.virtual_upa_id,
        head_of_geo.virtual_upa_name,
        head_of_geo.virtual_upa_owner_id,
        base.real_upa_id,
        base.real_upa_name,
        base.real_upa_owner_id,
        base.real_upa_owner_geo,
        base.account_id,
        base.account_name,
        base.account_owner_id
    FROM upa_virtual_cte AS base
    LEFT JOIN rank_virtual_upa_geo AS head_of_geo
        ON base.real_upa_id = head_of_geo.real_upa_id
            AND base.virtual_upa_owner_geo = head_of_geo.virtual_upa_owner_geo

-- in some cases some accounts can fall twice in different hierarchies
-- eg. TMobile

), unique_virtual_upa AS (

    SELECT DISTINCT
        real_upa_id,
        real_upa_name,
        real_upa_owner_id,
        real_upa_owner_geo,
        account_id,
        account_name,
        account_owner_id,
        MAX(virtual_upa_id) AS virtual_upa_id
    FROM adjusted_virtual_hierarchy AS base
    WHERE real_upa_owner_id != virtual_upa_owner_id
    GROUP BY 1,2,3,4,5,6,7

-- adjust the overall hierarchy to add the adjusted virtual upa fields
),
adjusted_account_hierarchy AS (

    SELECT
        -----------------------------------------
        -----------------------------------------
        base.upa_id,
        base.upa_name,
        base.upa_owner_id,
        base.upa_owner_name,
        base.upa_owner_geo, --upa_user_geo
        base.account_demographics_upa_geo,

        base.account_id,
        base.account_name,
        base.account_owner_id,
        base.account_owner_name,
        base.account_owner_geo,
        base.account_demographics_geo,

        -----------------------------------------
        -----------------------------------------
        -- pending refactor
        CASE
            WHEN virtual_hierarchy.virtual_upa_id IS NOT NULL
                THEN 'Virtual'
            ELSE 'Real'
        END                                     AS upa_type,

        CASE
            WHEN base.upa_id != virtual_hierarchy.virtual_upa_id
                    THEN 1
            ELSE 0
        END                                    AS is_virtual_upa_flag,

        COALESCE(upa_virtual.account_id, base.upa_id)                                   AS virtual_upa_id,
        COALESCE(upa_virtual.account_name, base.upa_name)                               AS virtual_upa_name,
        COALESCE(upa_virtual.account_owner_id, base.upa_owner_id)                       AS virtual_upa_owner_id,
        COALESCE(upa_virtual.account_owner_name,  base.upa_owner_name)                  AS virtual_upa_owner_name,
        ''                                                                              AS virtual_upa_owner_title_category,

        COALESCE(upa_virtual.account_owner_segment, base.upa_owner_segment)              AS virtual_upa_segment,
        COALESCE(upa_virtual.account_owner_business_unit, base.upa_owner_business_unit)  AS virtual_upa_business_unit,
        COALESCE(upa_virtual.account_owner_sub_bu, base.upa_owner_sub_bu)                AS virtual_upa_sub_business_unit,
        COALESCE(upa_virtual.account_owner_division, base.upa_owner_division)            AS virtual_upa_division,
        COALESCE(upa_virtual.account_owner_asm, base.upa_owner_asm)                      AS virtual_upa_asm,

        -----------------------------------------
        -----------------------------------------
        -- account upa owner fields

        COALESCE(upa_virtual.account_owner_geo, base.upa_owner_geo)                  AS virtual_upa_owner_geo, --virtual_upa_geo
        COALESCE(upa_virtual.account_owner_region, base.upa_owner_region)            AS virtual_upa_owner_region, --virtual_upa_region
        COALESCE(upa_virtual.account_owner_area, base.upa_owner_area)                AS virtual_upa_owner_area, --virtual_upa_area

        COALESCE(upa_virtual.account_billing_country_name,
                    base.account_demographics_upa_country_name)                     AS virtual_upa_country_name, --virtual_upa_country
        COALESCE(upa_virtual.account_billing_postal_code,
                    base.account_demographics_upa_postal_code)                      AS virtual_upa_zip_code,
        COALESCE(upa_virtual.account_billing_state_name,
                    base.account_demographics_upa_state_name)                       AS virtual_upa_state_name,
        COALESCE(upa_virtual.account_billing_state_code,
                    base.account_demographics_upa_state_code)                       AS virtual_upa_state_code,

        -----------------------------------------
        -----------------------------------------
        -- account demographics fields
        COALESCE(upa_virtual.account_owner_business_unit,
            base.account_demographics_upa_business_unit)        AS virtual_upa_ad_business_unit,
        COALESCE(upa_virtual.account_owner_geo,
            base.account_demographics_upa_geo)                  AS virtual_upa_ad_geo,
        COALESCE(upa_virtual.account_owner_region,
            base.account_demographics_upa_region)               AS virtual_upa_ad_region,
        COALESCE(upa_virtual.account_owner_area,
            base.account_demographics_upa_area)                 AS virtual_upa_ad_area,
        UPPER(COALESCE(upa_virtual.account_owner_segment,
            base.account_demographics_upa_sales_segment))       AS virtual_upa_ad_segment,

        COALESCE(upa_virtual.account_billing_country_name,
            base.account_demographics_upa_country_name)         AS virtual_upa_ad_country,
        COALESCE(upa_virtual.account_billing_state_name,
            base.account_demographics_upa_state_name)           AS virtual_upa_ad_state_name,
        COALESCE(upa_virtual.account_billing_state_code,
            base.account_demographics_upa_state_code)           AS virtual_upa_ad_state_code,
        COALESCE(upa_virtual.account_billing_postal_code,
            base.account_demographics_upa_postal_code)          AS virtual_upa_ad_zip_code,
        -----------------------------------------
        -----------------------------------------

        COALESCE(upa_virtual.account_industry, base.upa_industry)          AS virtual_upa_industry

    FROM base_account AS base
    LEFT JOIN unique_virtual_upa AS virtual_hierarchy
        ON
            base.upa_id = virtual_hierarchy.real_upa_id
            AND base.account_id = virtual_hierarchy.account_id
            -- avoid cases where the virtual upa is owned by the owner of the upa
            -- e.g.0018X000032WX48QAG
            --AND virtual_hierarchy.virtual_upa_owner_id != base.upa_owner_id
    LEFT JOIN base_account upa_virtual
        ON upa_virtual.account_id = virtual_hierarchy.virtual_upa_id

),
-- Calculate a flag to identify account groups with multiple owners
different_owners_per_virtual_upa AS (

    SELECT

        virtual_upa_id,
        COUNT(DISTINCT account_id) AS count_distinct_owners
    FROM adjusted_account_hierarchy
    GROUP BY 1
    HAVING count_distinct_owners > 1

),
wk_sales_report_bob_virtual_hierarchy AS (

SELECT
    base.*, CASE
    WHEN diff_owners.virtual_upa_id IS NOT NULL
    THEN 1
    ELSE 0
    END AS has_multiple_owners_flag
FROM adjusted_account_hierarchy AS base
    LEFT JOIN different_owners_per_virtual_upa AS diff_owners
ON base.virtual_upa_id = diff_owners.virtual_upa_id

)
SELECT *
FROM wk_sales_report_bob_virtual_hierarchy