{{ config(materialized='table') }}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('rpt_lead_to_revenue','rpt_lead_to_revenue')
]) }}

, rpt_lead_to_revenue_base AS ( 

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
        sales_qualified_source_name,

    --Opportunity Dates
        sales_accepted_date,

    --Account Data
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        
    --Bizible Fields
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,

    --Flags
        is_mql,
        is_sao
    FROM rpt_lead_to_revenue
    WHERE (account_demographics_geo != 'JIHU'
     OR account_demographics_geo IS null) 
     AND (crm_opp_owner_geo_stamped != 'JIHU'
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
        sales_qualified_source_name,
        inquiry_sum,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count
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
        sales_qualified_source_name,
        mql_sum,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count
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
        email_domain_type,
        lead_source,
        source_buckets,
        sales_qualified_source_name,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
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
        email_domain_type,
        lead_source,
        source_buckets,
        sales_qualified_source_name,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        'Inquiry' AS metric_type,
        COUNT(DISTINCT actual_inquiry) AS metric_value
    FROM inquiry_prep
    {{ dbt_utils.group_by(n=18) }}
  
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
        email_domain_type,
        lead_source,
        source_buckets,
        sales_qualified_source_name,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        'MQL' AS metric_type,
        COUNT(DISTINCT mqls) AS metric_value
    FROM mql_prep
    {{ dbt_utils.group_by(n=18) }}
    
 ), saos AS (
  
    SELECT
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        crm_opp_owner_sales_segment_stamped_clean AS sales_segment, 
        crm_opp_owner_geo_stamped AS geo,
        email_domain_type,
        lead_source,
        source_buckets,
        sales_qualified_source_name,
        opp_order_type AS order_type,
        sales_accepted_date,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        'SAO' AS metric_type,
        COUNT(DISTINCT saos) AS metric_value
    FROM sao_prep
    {{ dbt_utils.group_by(n=19) }}
    
  ), intermediate AS (

    SELECT 
        date_day,
        date_range_week,
        date_range_month,
        date_range_quarter,
        date_range_year,
        sales_segment,
        geo,
        order_type,
        lead_source,
        source_buckets,
        email_domain_type,
        sales_qualified_source_name,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
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
        sales_segment,
        geo,
        order_type,
        lead_source,
        source_buckets,
        email_domain_type,
        sales_qualified_source_name,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
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
        sales_segment,
        geo,
        order_type,
        lead_source,
        source_buckets,
        email_domain_type,
        sales_qualified_source_name,
        parent_crm_account_lam,
        parent_crm_account_lam_dev_count,
        bizible_marketing_channel,
        bizible_marketing_channel_path,
        bizible_medium,
        metric_type,
        metric_value
    FROM saos
    
), final AS (

  SELECT DISTINCT
    date_day,
    date_range_week,
    date_range_month,
    date_range_quarter,
    date_range_year,
    sales_segment,
    geo,
    order_type,
    lead_source,
    source_buckets,
    email_domain_type,
    sales_qualified_source_name,
    parent_crm_account_lam,
    parent_crm_account_lam_dev_count,
    bizible_marketing_channel,
    bizible_marketing_channel_path,
    bizible_medium,
    metric_type,
    metric_value
  FROM intermediate
  WHERE date_day IS NOT NULL
  
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2023-08-22",
    updated_date="2023-08-22",
  ) }}