{{ config(
    tags=["mnpi_exception"],
    materialized="table"
) }}

WITH bizible_person_touchpoint_source AS (

    SELECT *
    FROM {{ref('sfdc_bizible_touchpoint_source')}}
    WHERE is_deleted = 'FALSE'
    
), bizible_person_touchpoint_base AS (

  SELECT DISTINCT 
    bizible_person_touchpoint_source.*,
    LOWER(bizible_form_url) AS bizible_form_url_prep,
    REPLACE(bizible_form_url_prep,'.html','') AS bizible_form_url_clean,
    pathfactory_content_type,
    prep_campaign.type
  FROM bizible_person_touchpoint_source
  LEFT JOIN {{ ref('sheetload_bizible_to_pathfactory_mapping') }}  
    ON bizible_form_url_clean=bizible_url
  LEFT JOIN {{ ref('prep_campaign') }}
      ON bizible_person_touchpoint_source.campaign_id = prep_campaign.dim_campaign_id

), final AS (

  SELECT
    bizible_person_touchpoint_base.*,
    CASE   
      WHEN bizible_touchpoint_type = 'Web Chat' OR LOWER(bizible_ad_campaign_name) LIKE '%webchat%' 
        THEN 'Web Chat'
      WHEN bizible_touchpoint_type = 'Web Form' 
        AND bizible_form_url_clean IN ('gitlab.com/-/trial_registrations/new',
                                'gitlab.com/-/trial_registrations',
                                'gitlab.com/-/trials/new')
        THEN 'GitLab Dot Com Trial'
      WHEN bizible_touchpoint_type = 'Web Form' 
        AND bizible_form_url_clean IN (
                                'about.gitlab.com/free-trial/index',
                                'about.gitlab.com/free-trial',
                                'about.gitlab.com/free-trial/self-managed',
                                'about.gitlab.com/free-trial/self-managed/index'
                                )
        THEN 'GitLab Self-Managed Trial'
      WHEN  bizible_form_url_clean LIKE '%/sign_up%' 
        THEN 'Sign Up Form'
      WHEN bizible_form_url_clean = 'about.gitlab.com/sales' 
        THEN 'Contact Sales Form' 
      WHEN bizible_form_url_clean LIKE '%/resources/%' 
        THEN 'Resources'
      WHEN bizible_touchpoint_type = 'Web Form' AND (bizible_form_url_clean LIKE '%page.gitlab.com%') 
        THEN 'Online Content' 
      WHEN bizible_form_url_clean LIKE 'learn.gitlab.com%' 
        THEN 'Online Content'
      WHEN bizible_marketing_channel = 'Event' 
        THEN TYPE 
      WHEN LOWER(bizible_ad_campaign_name) like '%lead%' AND LOWER(bizible_ad_campaign_name) LIKE '%linkedin%' 
        THEN 'Lead Gen Form'
      WHEN bizible_marketing_channel = 'IQM' 
        THEN 'IQM'
      WHEN bizible_form_url_clean LIKE '%/education/%' OR LOWER(bizible_ad_campaign_name) LIKE '%education%' 
        THEN 'Education'
      WHEN bizible_form_url_clean LIKE '%/company/%' 
        THEN 'Company Pages'
      WHEN bizible_touchpoint_type = 'CRM' AND (LOWER(bizible_ad_campaign_name) LIKE '%partner%' OR LOWER(bizible_medium) LIKE '%partner%')
        THEN 'Partner Sourced'  
      WHEN bizible_form_url_clean ='about.gitlab.com/company/contact/' OR bizible_form_url_clean LIKE '%/releases/%' OR bizible_form_url_clean LIKE '%/blog/%' 
        THEN 'Newsletter/Release/Blog Sign-Up'
      WHEN type = 'Survey' OR bizible_form_url_clean LIKE '%survey%' OR LOWER(bizible_ad_campaign_name) LIKE '%survey%' 
        THEN 'Survey'
      WHEN bizible_form_url_clean LIKE '%/solutions/%' OR bizible_form_url_clean IN ('https://about.gitlab.com/enterprise/','https://about.gitlab.com/small-business/') 
        THEN 'Solutions' 
      WHEN bizible_form_url_clean LIKE '%/blog%'
        THEN 'Blog'
      WHEN bizible_form_url_clean LIKE '%/webcast/%' 
        THEN 'Webcast'
      WHEN bizible_form_url_clean LIKE '%/services%'  
        THEN 'GitLab Professional Services' 
      WHEN bizible_form_url_clean LIKE '%/fifteen%'  
        THEN  'Event Registration' 
      WHEN bizible_ad_campaign_name LIKE '%PQL%' 
        THEN 'Product Qualified Lead'
      WHEN bizible_marketing_channel_path LIKE '%Content Syndication%' 
        THEN 'Content Syndication'
      ELSE 'Other'
    END AS touchpoint_offer_type_wip,
    CASE 
      WHEN pathfactory_content_type IS NOT NULL 
        THEN pathfactory_content_type
      WHEN touchpoint_offer_type_wip = 'Online Content' AND bizible_form_url_clean LIKE '%registration-page%' OR bizible_form_url_clean LIKE '%registrationpage%' OR bizible_form_url_clean LIKE '%inperson%' OR bizible_form_url_clean LIKE '%in-person%' OR bizible_form_url_clean LIKE '%landing-page%'
        THEN 'Event Registration'
      WHEN touchpoint_offer_type_wip = 'Online Content' AND bizible_form_url_clean LIKE '%ebook%'
        THEN 'eBook'
      WHEN touchpoint_offer_type_wip = 'Online Content' AND bizible_form_url_clean LIKE '%webcast%'
        THEN 'Webcast'
      WHEN touchpoint_offer_type_wip = 'Online Content' AND bizible_form_url_clean LIKE '%demo%' 
        THEN 'Demo'
      WHEN touchpoint_offer_type_wip = 'Online Content'
        THEN 'Other Online Content'
      ELSE touchpoint_offer_type_wip
    END AS touchpoint_offer_type,
    CASE
      WHEN bizible_marketing_channel = 'Event' OR touchpoint_offer_type = 'Event Registration' OR touchpoint_offer_type = 'Webcast' OR touchpoint_offer_type = 'Workshop' 
        THEN 'Events'
      WHEN touchpoint_offer_type_wip = 'Online Content' 
        THEN 'Online Content'
      WHEN touchpoint_offer_type IN ('Content Syndication','Newsletter/Release/Blog Sign-Up','Blog','Resources')
        THEN 'Online Content'
      WHEN touchpoint_offer_type = 'Sign Up Form' 
        THEN 'Sign Up Form'
      WHEN touchpoint_offer_type in ('GitLab Dot Com Trial', 'GitLab Self-Managed Trial')
        THEN 'Trials'
      WHEN touchpoint_offer_type = 'Web Chat' 
        THEN 'Web Chat'
      WHEN touchpoint_offer_type = 'Contact Sales Form' 
        THEN 'Contact Sales Form'
      WHEN touchpoint_offer_type = 'Partner Sourced' 
        THEN 'Partner Sourced'
      WHEN touchpoint_offer_type = 'Lead Gen Form' 
        THEN 'Lead Gen Form'
      ELSE 'Other'
    END AS touchpoint_offer_type_grouped
  FROM bizible_person_touchpoint_base

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-01-31",
    updated_date="2024-01-31"
) }}