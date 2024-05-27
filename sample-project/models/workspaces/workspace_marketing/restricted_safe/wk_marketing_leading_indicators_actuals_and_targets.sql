{{ config(materialized='table') }}

{{ simple_cte([
    ('mart_sales_funnel_target_daily','mart_sales_funnel_target_daily'),
    ('dim_date','dim_date'),
    ('rpt_lead_to_revenue','rpt_lead_to_revenue')
]) }}

, targets AS (
  
  SELECT 
    target_date,
    target_month,
    first_day_of_week,
    fiscal_quarter_name,
    fiscal_year,
    CASE
      WHEN kpi_name = 'Stage 1 Opportunities' THEN 'SAO_TARGET'
      WHEN kpi_name = 'MQL' THEN 'MQL_TARGET'
      ELSE NULL
    END AS kpi_name, 
    crm_user_sales_segment,
    crm_user_geo,
    crm_user_region,
    crm_user_area,
    order_type_name,
    sales_qualified_source_name,
    daily_allocated_target,
    wtd_allocated_target,
    mtd_allocated_target,
    qtd_allocated_target,
    ytd_allocated_target
  FROM mart_sales_funnel_target_daily 
  WHERE kpi_name IN ('MQL','Stage 1 Opportunities')
  
), rpt_lead_to_revenue_base AS ( 

    SELECT
    --IDs    
        dim_crm_person_id,
        dim_crm_opportunity_id,

    --Person Data
        email_hash,
        email_domain_type,
        person_order_type,
        account_demographics_sales_segment,
        account_demographics_geo,
        account_demographics_area,
        account_demographics_region,
        lead_source,
        source_buckets,
        inquiry_sum,
        mql_sum, 

    --Person Dates
        true_inquiry_date,
        mql_date_first_pt,
        mql_date_latest_pt,

    --Opportunity Data
        opp_order_type,
        crm_opp_owner_sales_segment_stamped,
        crm_opp_owner_geo_stamped,
        crm_opp_owner_region_stamped,
        crm_opp_owner_area_stamped,
        sales_qualified_source_name,
        opp_lead_source,
        opp_source_buckets,

    --Opportunity Dates
        sales_accepted_date,

    --Account Data
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,

    --Flags
        is_mql,
        is_sao
    FROM rpt_lead_to_revenue
    WHERE (account_demographics_geo != 'JIHU'
     OR account_demographics_geo IS null) 
     OR (crm_opp_owner_geo_stamped != 'JIHU'
     OR crm_opp_owner_geo_stamped IS null)

), date_base AS (

    SELECT
        date_day,
        fiscal_year                     AS date_range_year,
        fiscal_quarter_name_fy          AS date_range_quarter,
        first_day_of_month              AS date_range_month,
        first_day_of_week               AS date_range_week
    FROM dim_date

), inquiry_prep AS (

    SELECT
        date_base.*,
        true_inquiry_date,
        CASE 
            WHEN true_inquiry_date IS NOT null 
                THEN email_hash
            ELSE null
        END AS actual_inquiry,
        email_domain_type,
        person_order_type,
        account_demographics_sales_segment,
        account_demographics_geo,
        lead_source,
        source_buckets,
        NULL AS sales_qualified_source_name,
        inquiry_sum,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        account_demographics_area,
        account_demographics_region
    FROM rpt_lead_to_revenue_base
    LEFT JOIN date_base
        ON rpt_lead_to_revenue_base.true_inquiry_date=date_base.date_day    
    WHERE 1=1
    AND (account_demographics_geo != 'JIHU'
        OR account_demographics_geo IS null)

 ), mql_prep AS (
     
    SELECT
        date_base.*,
        is_mql,
        CASE 
        WHEN is_mql = true THEN email_hash
        ELSE null
        END AS mqls,
        email_domain_type,
        person_order_type,
        account_demographics_sales_segment,
        account_demographics_geo,
        lead_source,
        source_buckets,
        NULL AS sales_qualified_source_name,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        account_demographics_area,
        account_demographics_region
  FROM rpt_lead_to_revenue_base
  LEFT JOIN date_base
    ON rpt_lead_to_revenue_base.mql_date_latest_pt=date_base.date_day
  WHERE 1=1 
   AND (account_demographics_geo != 'JIHU'
     OR account_demographics_geo IS null) 
  
), sao_prep AS (
     
    SELECT
        date_base.*,
        is_sao,
        opp_order_type,
        CASE 
            WHEN crm_opp_owner_sales_segment_stamped = 'LARGE' 
                THEN 'Large'
            WHEN crm_opp_owner_sales_segment_stamped = 'MID-MARKET' 
                THEN 'Mid-Market'
            WHEN crm_opp_owner_sales_segment_stamped = 'PUBSEC' 
                THEN 'PubSec'
            WHEN crm_opp_owner_sales_segment_stamped = 'OTHER' 
                THEN 'Other'
            ELSE crm_opp_owner_sales_segment_stamped
        END AS crm_opp_owner_sales_segment_stamped_clean,
        crm_opp_owner_geo_stamped,
        opp_lead_source,
        opp_source_buckets,
        sales_qualified_source_name,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        crm_opp_owner_region_stamped,
        crm_opp_owner_area_stamped,
        CASE 
            WHEN is_sao = true 
                THEN dim_crm_opportunity_id 
            ELSE null 
        END AS saos,
        sales_accepted_date
    FROM rpt_lead_to_revenue_base
    LEFT JOIN date_base 
        ON rpt_lead_to_revenue_base.sales_accepted_date=date_base.date_day
    WHERE 1=1
        AND sales_accepted_date <= CURRENT_DATE
        AND (crm_opp_owner_geo_stamped != 'JIHU'
        OR crm_opp_owner_geo_stamped IS null)

), inquiries AS (

    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        person_order_type as order_type,
        account_demographics_sales_segment AS sales_segment,
        account_demographics_geo AS geo,
        account_demographics_area AS area,
        account_demographics_region AS region,
        sales_qualified_source_name,
        lead_source,
        source_buckets,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        'Inquiry' AS metric_type,
        COUNT(DISTINCT actual_inquiry) AS metric_value
    FROM inquiry_prep
    {{ dbt_utils.group_by(n=16) }}
  
), mqls AS (

    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        person_order_type as order_type,
        account_demographics_sales_segment AS sales_segment,
        account_demographics_geo AS geo,
        account_demographics_area AS area,
        account_demographics_region AS region,
        sales_qualified_source_name,
        lead_source,
        source_buckets,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        'MQL' AS metric_type,
        COUNT(DISTINCT mqls) AS metric_value
    FROM mql_prep
    {{ dbt_utils.group_by(n=16) }}
    
 ), saos AS (
  
    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        crm_opp_owner_sales_segment_stamped_clean AS sales_segment, 
        crm_opp_owner_geo_stamped AS geo,
        crm_opp_owner_region_stamped AS region,
        crm_opp_owner_area_stamped AS area,
        sales_qualified_source_name,
        opp_order_type AS order_type,
        opp_lead_source,
        opp_source_buckets,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        'SAO' AS metric_type,
        COUNT(DISTINCT saos) AS metric_value
    FROM sao_prep
    {{ dbt_utils.group_by(n=16) }}
    
  ), intermediate AS (

    SELECT 
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        order_type,
        sales_segment,
        geo,
        area,
        region,
        sales_qualified_source_name,
        lead_source,
        source_buckets,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        metric_type,
        metric_value
    FROM inquiries
    UNION ALL
    SELECT 
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        order_type,
        sales_segment,
        geo,
        area,
        region,
        sales_qualified_source_name,
        lead_source,
        source_buckets,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        metric_type,
        metric_value
    FROM mqls
    UNION ALL
    SELECT 
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        order_type,
        sales_segment,
        geo,
        area,
        region,
        sales_qualified_source_name,
        opp_lead_source,
        opp_source_buckets,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        metric_type,
        metric_value
    FROM saos
    
), combined AS (

  SELECT DISTINCT
    date_day,
    date_range_week,
    date_range_month,
    date_range_quarter,
    date_range_year,
    order_type,
    sales_segment,
    geo,
    area,
    region,
    sales_qualified_source_name,
    lead_source,
    source_buckets,
    parent_crm_account_lam,
    parent_crm_account_lam_dev_count,
    metric_type,
    metric_value,
    'Actual' AS metric_type_flag
  FROM intermediate
  UNION ALL
  SELECT
    target_date,
    first_day_of_week,
    target_month,
    fiscal_quarter_name,
    fiscal_year, 
    order_type_name,
    crm_user_sales_segment,
    crm_user_geo,
    crm_user_area,
    crm_user_region,
    sales_qualified_source_name,
    NULL AS lead_source,
    NULL AS source_buckets,
    NULL AS parent_crm_account_lam,
    NULL AS parent_crm_account_lam_dev_count,
    kpi_name AS metric_type,
    daily_allocated_target AS metric_value,
    'Target' AS metric_type_flag
  FROM targets

), base AS (

     SELECT * 
     FROM combined
     WHERE date_day > '2021-01-31'

), regroup_actuals AS (

SELECT 
    date_day,
    date_range_quarter,
    date_range_year,
    order_type,
    sales_segment,
    geo,
    region,
    area,
    source_buckets AS lead_source_buckets,
    lead_source, 
    sales_qualified_source_name, 
    metric_type_flag,
    metric_type,
    CASE 
        WHEN CONTAINS(METRIC_TYPE, 'Inquiry') THEN 'INQs'
        WHEN CONTAINS(METRIC_TYPE, 'MQL') THEN 'MQLs'
        WHEN CONTAINS(METRIC_TYPE, 'SAO') THEN 'SAOs'
    END AS metric_name,
    SUM(metric_value) AS metric_value
FROM base
WHERE 1=1 
    AND NOT CONTAINS(metric_type_flag, 'Target')
{{ dbt_utils.group_by(n=14) }}

), regroup_targets AS (

SELECT 
    date_day,
    date_range_quarter,
    date_range_year,
    order_type,
    sales_segment,
    geo,
    region,
    area,
    null AS lead_source_buckets,
    null AS lead_source, 
    sales_qualified_source_name, 
    metric_type_flag,
    metric_type,
    CASE 
        WHEN CONTAINS(METRIC_TYPE, 'Inquiry') THEN 'INQs'
        WHEN CONTAINS(METRIC_TYPE, 'MQL') THEN 'MQLs'
        WHEN CONTAINS(METRIC_TYPE, 'SAO') THEN 'SAOs'
    END AS metric_name,
    SUM(metric_value) AS metric_value
FROM base
WHERE 1=1 
    AND CONTAINS(metric_type_flag, 'Target')
{{ dbt_utils.group_by(n=14) }}

), final AS (

SELECT 
    date_day,
    date_range_quarter,
    date_range_year,
    order_type,
    sales_segment,
    geo,
    region,
    area,
    lead_source_buckets,
    lead_source,
    sales_qualified_source_name,
    metric_type_flag,
    metric_type,
    metric_name,
    metric_value
FROM regroup_actuals
UNION ALL 
SELECT 
    date_day,
    date_range_quarter,
    date_range_year,
    order_type,
    sales_segment,
    geo,
    region,
    area,
    lead_source_buckets,
    lead_source,
    sales_qualified_source_name,
    metric_type_flag,
    metric_type,
    metric_name,
    metric_value 
FROM regroup_targets

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-08-22",
    updated_date="2023-12-05",
  ) }}

