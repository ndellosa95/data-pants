WITH find_current_campaigns AS (
  SELECT
    *,
    MAX(
      LAST_MODIFIED_TIME
    ) OVER (PARTITION BY campaign_id ORDER BY LAST_MODIFIED_TIME DESC) AS latest_update,
    latest_update = LAST_MODIFIED_TIME AS is_latest
  FROM {{ ref('linkedin_ads_campaign_history_source') }}

),current_campaigns AS (
  SELECT
    *
  FROM find_current_campaigns
  WHERE is_latest

), find_current_creatives AS (
  SELECT
    *,
    MAX(
      LAST_MODIFIED_TIME
    ) OVER (PARTITION BY creative_id ORDER BY LAST_MODIFIED_TIME DESC) AS latest_update,
    latest_update = LAST_MODIFIED_TIME AS is_latest
   FROM {{ ref('linkedin_ads_creative_history_source') }}

),current_creatives AS (
  SELECT
    *
  FROM find_current_creatives
  WHERE is_latest

), creative_stats as (
  SELECT
    *
   FROM {{ ref('linkedin_ads_ad_analytics_by_creative_source') }}
)

SELECT
    
    /* Account Info */
     current_campaigns.account_id,
    /* Campaign Info */
     
    current_campaigns.campaign_id         AS campaign_id,
    current_campaigns.name                AS campaign_name,
    current_campaigns.status              AS campaign_status,
    current_campaigns.RUN_SCHEDULE_END    AS campaign_end_date,
    current_campaigns.RUN_SCHEDULE_START  AS campaign_start_date,
    current_campaigns.OBJECTIVE_TYPE      AS ad_type,

    /* Creative Info */

    current_creatives.type                AS creative_type,
    current_creatives.status              AS creative_status,
    current_creatives.click_uri,
    current_creatives.text_ad_text,
    current_creatives.text_ad_title,
    
    /* Creative Stats */
    creative_stats.day                    AS campaign_day,
    creative_stats.impressions,
    creative_stats.clicks,
    creative_stats.ONE_CLICK_LEADS        AS linkedin_leads,
    creative_stats.cost_in_usd
FROM
current_campaigns
    LEFT JOIN current_creatives ON current_campaigns.campaign_id = current_creatives.campaign_id
    LEFT JOIN creative_stats ON current_creatives.creative_id = creative_stats.creative_id
