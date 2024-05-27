{{ config(
    tags=["mnpi_exception"],
    materialized='table'
) }}

{{ simple_cte([
    ('ga360_session_hit','ga360_session_hit'),
    ('ga360_session','ga360_session'),
    ('rpt_namespace_onboarding','rpt_namespace_onboarding'), 
    ('fct_behavior_website_page_view','fct_behavior_website_page_view'),
    ('dim_date','dim_date')
]) }}

, site_sessions_base AS (

    SELECT
      CASE
        WHEN traffic_source_medium LIKE '%paidsocial%' THEN 'Paid Social'
        ELSE channel_grouping
      END AS channel_grouping,
      ga360_session.visit_start_time,
      total_new_visits IS NOT NULL AS is_new_user,
      ga360_session.visitor_id,
      ga360_session.visit_id,
      ga360_session.client_id
    FROM ga360_session_hit 
    LEFT JOIN ga360_session
        ON ga360_session_hit.visit_id = ga360_session.visit_id 
            AND ga360_session_hit.visitor_id = ga360_session.visitor_id
    WHERE ga360_session.visit_start_time >= '2022-01-01'   
        AND LOWER(ga360_session_hit.hit_type) = 'page'
        AND ga360_session_hit.host_name IN ('about.gitlab.com')
        AND LOWER(ga360_session_hit.page_path) NOT LIKE '%/hANDbook%' 
        AND LOWER(ga360_session_hit.page_path) NOT LIKE '%/job%'
        AND LOWER(ga360_session_hit.page_path) NOT LIKE '%/unsubscribe%'
        AND LOWER(ga360_session_hit.is_entrance) = 'true'

), site_sessions AS (
    
    SELECT
        DATE_TRUNC('month', visit_start_time)        AS metric_time,
        'site_sessions'                              AS metric_name,
        channel_grouping,
        COUNT(DISTINCT site_sessions_base.visitor_id, site_sessions_base.visit_id) AS metric_value
    FROM site_sessions_base
    GROUP BY 1,2,3

), page_view_sign_in AS (

    SELECT
     visitor_id,
     visit_id
    FROM ga360_session_hit
    WHERE visit_start_time >= '2022-01-01'  
        AND LOWER(hit_type) = 'page'
        AND LOWER(page_path) LIKE '/users/sign_in%'
    GROUP BY 1, 2

), site_sessions_exclude_signin AS (

    SELECT
        DATE_TRUNC('month', visit_start_time)        AS metric_time,
        'site_sessions_exclude_signin'               AS metric_name,
        channel_grouping,
        COUNT(DISTINCT site_sessions_base.visitor_id, site_sessions_base.visit_id)         AS metric_value
    FROM site_sessions_base
    LEFT JOIN page_view_sign_in
        ON site_sessions_base.visitor_id = page_view_sign_in.visitor_id
            AND site_sessions_base.visit_id = page_view_sign_in.visit_id
    WHERE page_view_sign_in.visit_id IS NULL 
        AND page_view_sign_in.visitor_id IS NULL
    GROUP BY  1,2,3

), new_users AS (

    SELECT
        DATE_TRUNC('month', visit_start_time)        AS metric_time,
        'new_users'                                  AS metric_name,
        channel_grouping,
        COUNT(DISTINCT client_id)                    AS metric_value
    FROM site_sessions_base
    WHERE is_new_user
    GROUP BY 1,2,3

), new_users_exclude_signin AS (

    SELECT
        DATE_TRUNC('month', visit_start_time)        AS metric_time,
        'new_users_exclude_signin'                   AS metric_name,
        channel_grouping,
        COUNT(DISTINCT client_id)                    AS metric_value
    FROM site_sessions_base
    LEFT JOIN page_view_sign_in
        ON site_sessions_base.visitor_id = page_view_sign_in.visitor_id
            AND site_sessions_base.visit_id = page_view_sign_in.visit_id
    WHERE page_view_sign_in.visit_id IS NULL 
        AND page_view_sign_in.visitor_id IS NULL
        AND is_new_user
    GROUP BY 1,2,3

-- engaged_sessions, could pull FROM the a seperate SELECT but thIS woks AS well
), engaged_session AS (

    SELECT 
        DATE_TRUNC('month', ga360_session.visit_start_time)              AS metric_time,
        'engaged_sessions'                                               AS metric_name,
        CASE
            WHEN traffic_source_medium
            LIKE '%paidsocial%' THEN 'Paid Social'
            ELSE channel_grouping
        END                                                              AS channel_grouping,
        COUNT(DISTINCT ga360_session.visitor_id, ga360_session.visit_id) AS metric_value
    FROM ga360_session_hit
    LEFT JOIN ga360_session
        ON ga360_session_hit.visit_id = ga360_session.visit_id 
            AND ga360_session_hit.visitor_id = ga360_session.visitor_id    
    WHERE ga360_session.visit_start_time >= '2022-01-01'  
        AND LOWER(ga360_session_hit.hit_type) LIKE 'event'
        AND ga360_session_hit.host_name IN ('about.gitlab.com')
        AND LOWER(ga360_session_hit.event_category) IN ('cta click')
        AND LOWER(ga360_session_hit.event_action) NOT LIKE 'in-line'
        AND (ga360_session_hit.custom_dimensions:"Click URL" LIKE '%about.gitlab.com/sales/%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%gitlab.com/-/trials/new%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%/solutions/gitlab-duo-pro/sales%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%gitlab.com/-/trial_registrations/new%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%customers.gitlab.com/subscriptions/new?plan_id=2c92a01176f0d50a0176f3043c4d4a53%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%customers.gitlab.com/subscriptions/new?plan_id=2c92a00c76f0c6c20176f2f9328b33c9%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%gitlab.com/-/subscriptions/new?plan_id=2c92a0ff76f0d5250176f2f8c86f305a%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%gitlab.com/-/subscriptions/new?plan_id=2c92a00d76f0d5060176f2fb0a5029ff%'
            )
    GROUP BY 1,2,3

), engaged_session_exclude_signin AS (

    SELECT 
        DATE_TRUNC('month', ga360_session.visit_start_time)              AS metric_time,
        'engaged_sessions_exclude_signin'                                AS metric_name,
        CASE
            WHEN traffic_source_medium
            LIKE '%paidsocial%' THEN 'Paid Social'
            ELSE channel_grouping
        END                                                              AS channel_grouping,
        COUNT(DISTINCT ga360_session.visitor_id, ga360_session.visit_id) AS metric_value
    FROM ga360_session_hit
    LEFT JOIN ga360_session
        ON ga360_session_hit.visit_id = ga360_session.visit_id 
            AND ga360_session_hit.visitor_id = ga360_session.visitor_id
    LEFT JOIN page_view_sign_in
        ON ga360_session.visitor_id = page_view_sign_in.visitor_id
            AND ga360_session.visit_id = page_view_sign_in.visit_id
    WHERE page_view_sign_in.visit_id IS NULL 
        AND page_view_sign_in.visitor_id IS null
        AND ga360_session.visit_start_time >= '2022-01-01'  
        AND LOWER(ga360_session_hit.hit_type) LIKE 'event'
        AND ga360_session_hit.host_name IN ('about.gitlab.com')
        AND LOWER(ga360_session_hit.event_category) IN ('cta click')
        AND LOWER(ga360_session_hit.event_action) NOT LIKE 'in-line'
        AND (ga360_session_hit.custom_dimensions:"Click URL" LIKE '%about.gitlab.com/sales/%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%gitlab.com/-/trials/new%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%/solutions/gitlab-duo-pro/sales%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%gitlab.com/-/trial_registrations/new%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%customers.gitlab.com/subscriptions/new?plan_id=2c92a01176f0d50a0176f3043c4d4a53%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%customers.gitlab.com/subscriptions/new?plan_id=2c92a00c76f0c6c20176f2f9328b33c9%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%gitlab.com/-/subscriptions/new?plan_id=2c92a0ff76f0d5250176f2f8c86f305a%'
            OR ga360_session_hit.custom_dimensions:"Click URL" LIKE '%gitlab.com/-/subscriptions/new?plan_id=2c92a00d76f0d5060176f2fb0a5029ff%'
            )
    GROUP BY 1,2,3

), ga_saas_trial_signup AS (

    SELECT 
        DATE_TRUNC('month', site_sessions_base.visit_start_time)      AS metric_time,
        'ga_saas_trial_signup'                       AS metric_name,
        channel_grouping,
        COUNT(DISTINCT site_sessions_base.visitor_id, site_sessions_base.visit_id)     AS metric_value
    FROM ga360_session_hit
    LEFT JOIN site_sessions_base
        ON ga360_session_hit.visit_id = site_sessions_base.visit_id 
            AND ga360_session_hit.visitor_id = site_sessions_base.visitor_id    
    WHERE LOWER(ga360_session_hit.hit_type) LIKE 'event'
        AND ga360_session_hit.host_name LIKE 'gitlab.com'
        AND ga360_session_hit.event_category LIKE 'SaAS Free Trial'
        AND (ga360_session_hit.event_actiON LIKE 'About Your Company' OR ga360_session_hit.event_actiON LIKE 'Form Submit')
        AND (ga360_session_hit.event_label LIKE 'ultimate_trial' OR ga360_session_hit.event_label IS NULL)        
    GROUP BY 1,2,3

--Pages Per Session
), sessions_with_page_count AS (

    SELECT 
        visit_id,
        visitor_id,
        MAX(total_pageviews) AS total_pageviews
    FROM ga360_session
    GROUP BY 1,2

), site_sessions_base_grouped AS (

    -- GROUP  the site_sessions_base so it joins with the page views
    SELECT
        visit_start_time,
        visitor_id,
        visit_id
    FROM site_sessions_base
    GROUP BY 1,2,3

), pages_per_session AS (

    SELECT
        DATE_TRUNC('month', visit_start_time)    AS metric_time,
        'pages_per_session'                      AS metric_name,
        NULL                                     AS channel_grouping,
        AVG(total_pageviews)                     AS metric_value 
    FROM site_sessions_base_grouped
    LEFT JOIN sessions_with_page_count 
        ON site_sessions_base_grouped.visit_id = sessions_with_page_count.visit_id 
        AND site_sessions_base_grouped.visitor_id = sessions_with_page_count.visitor_id
    GROUP BY 1,2,3

), pages_per_session_exclude_signin AS (

    SELECT
        DATE_TRUNC('month', visit_start_time)    AS metric_time,
        'pages_per_session_exclude_signin'       AS metric_name,
        NULL                                     AS channel_grouping,
        AVG(total_pageviews)                     AS metric_value 
    FROM site_sessions_base_grouped
    LEFT JOIN sessions_with_page_count 
        ON site_sessions_base_grouped.visit_id = sessions_with_page_count.visit_id 
            AND site_sessions_base_grouped.visitor_id = sessions_with_page_count.visitor_id
    LEFT JOIN page_view_sign_in
        ON site_sessions_base_grouped.visit_id = page_view_sign_in.visit_id 
            AND site_sessions_base_grouped.visitor_id = page_view_sign_in.visitor_id
    WHERE page_view_sign_in.visit_id IS NULL 
        AND page_view_sign_in.visitor_id IS NULL
    GROUP BY 1,2,3

-- snowplow from here down

), trials_base AS (

    SELECT
        ultimate_parent_namespace_id,
        trial_start_date,
        creator_is_valuable_signup,
        namespace_contains_valuable_signup,
        has_team_activation,
        first_paid_subscription_start_date,
        is_namespace_created_within_2min_of_creator_invite_acceptance
    FROM rpt_namespace_onboarding
    WHERE is_namespace_created_within_2min_of_creator_invite_acceptance = false

), snowplow_page_views_base AS (

    SELECT
        session_id,
        user_snowplow_domain_id,
        gsc_namespace_id,
        page_view_start_at,
        gsc_plan,
        page_url,
        event_id        AS page_view_id,
        page_url_path,
        page_url_host
    FROM fct_behavior_website_page_view
    WHERE page_view_start_at >= '2022-01-01'  
        -- marketing
        AND ((page_url_host = 'about.gitlab.com'
        AND page_url NOT LIKE '%/job%'
        AND page_url NOT LIKE '%/hANDbook%'
        AND page_url NOT LIKE '%/unsubscribe%'
        ) OR (
        -- product
            page_url_host LIKE 'gitlab.com'
        ))
        
), snowplow_page_views_marketing AS (

    SELECT
        session_id,
        user_snowplow_domain_id,
        MIN(page_view_start_at) AS first_marketing_page_view
    FROM snowplow_page_views_base
    WHERE page_url_host = 'about.gitlab.com'
    GROUP BY 1,2

-- Pages per Trial
), trial_namespace_ids_from_starts AS (

    --namespace id only available for certain gitlab.com pages
    -- additional we need to ensure we are joining to a namespace id instead of aNOTher type of ID
    SELECT DISTINCT
        page_view_id,
        gsc_namespace_id,
        user_snowplow_domain_id,
        creator_is_valuable_signup,
        trial_start_date
    FROM snowplow_page_views_base
    JOIN trials_base 
        ON snowplow_page_views_base.gsc_namespace_id = trials_base.ultimate_parent_namespace_id 
            AND page_url_host LIKE 'gitlab.com'
            -- trial date IS equal to or after the pageview date
            AND trial_start_date >= DATE_TRUNC(DAY, page_view_start_at)
            -- trial AND page views must have happened in the same month
            AND DATE_TRUNC('month', trial_start_date) = DATE_TRUNC('month',page_view_start_at)

), pages_per_trial_start AS (

    -- counting pageviews for trialed users who viewed the marketing site 
    SELECT
        DATE_TRUNC('month', trial_start_date)        AS metric_time,
        'pages_per_trial_start'                      AS metric_name,
        NULL                                         AS channel_grouping,
        COUNT(DISTINCT snowplow_page_views_base.page_view_id) / COUNT(DISTINCT snowplow_page_views_base.user_snowplow_domain_id) AS metric_value
        -- this should be selecting from the marketing page view cte, but it has a min AND 
        -- it is easier to rebuild it rather than create AND debug something else
    FROM snowplow_page_views_base 
    JOIN trial_namespace_ids_FROM_starts
        ON trial_namespace_ids_FROM_starts.user_snowplow_domain_id = snowplow_page_views_base.user_snowplow_domain_id
            -- trial AND page views have to have been in the same month
            AND DATE_TRUNC('month', trial_namespace_ids_FROM_starts.trial_start_date) 
                = DATE_TRUNC('month',snowplow_page_views_base.page_view_start_at)
    WHERE page_url_host = 'about.gitlab.com'
    GROUP BY 1,2,3

--Saas Trial Pageviews
-- start on marketing site then move to product trial page in the same session
-- linked by session so i'm not worried about old page views coming back and haulting us

), snowplow_page_views_trials AS (

    SELECT 
        DATE_TRUNC('month', page_view_start_at)         AS metric_time,
        'saas_trial_pages_views_users'               AS metric_name,
        NULL                                         AS channel_grouping,
        COUNT(DISTINCT snowplow_page_views_base.user_snowplow_domain_id) AS metric_value
    FROM snowplow_page_views_base
    JOIN snowplow_page_views_marketing 
        ON snowplow_page_views_base.session_id = snowplow_page_views_marketing.session_id 
    WHERE page_view_start_at >= '2022-01-01'  
        AND (snowplow_page_views_base.page_url LIKE '%/trial_registrations/new%' 
            OR snowplow_page_views_base.page_url LIKE '%/trials/new%')
        -- start ON the marketing site, then move to the product
        AND page_view_start_at >= first_marketing_page_view
        AND DATE_TRUNC('month', page_view_start_at) = DATE_TRUNC('month', first_marketing_page_view)
    GROUP BY 1,2,3

--Saas Trial Signups
-- start on marketing site and create a trial
-- linked by session so i'm not worried about old page views coming back and haulting us

), namespaces_in_trial_touched_marketing_site AS (

    SELECT DISTINCT
        gsc_namespace_id,
        MIN(page_view_start_at) AS page_view_start_at
    FROM snowplow_page_views_base
    JOIN snowplow_page_views_marketing
        ON snowplow_page_views_base.session_id = snowplow_page_views_marketing.session_id
            AND snowplow_page_views_base.page_view_start_at >= snowplow_page_views_marketing.first_marketing_page_view
            AND DATE_TRUNC('month', snowplow_page_views_base.page_view_start_at) = DATE_TRUNC('month', snowplow_page_views_marketing.first_marketing_page_view)
    WHERE gsc_plan LIKE '%trial%'
    GROUP BY 1

), snowplow_trial_signup_namespaces AS (

    SELECT  
        DATE_TRUNC('month', trial_start_date)        AS metric_time,
        'saas_trial_signups_namespace_marketing'     AS metric_name,
        NULL                                         AS channel_grouping,
        COUNT(DISTINCT ultimate_parent_namespace_id) AS metric_value
    FROM trials_base
    JOIN namespaces_in_trial_touched_marketing_site
        ON trials_base.ultimate_parent_namespace_id = namespaces_in_trial_touched_marketing_site.gsc_namespace_id
    WHERE trial_start_date >= DATE_TRUNC(day, page_view_start_at)
        AND DATE_TRUNC('month', trial_start_date) = DATE_TRUNC('month', page_view_start_at)
    GROUP BY 1,2,3

), snowplow_trial_signup_namespaces_vs AS (

    SELECT  
        DATE_TRUNC('month', trial_start_date)        AS metric_time,
        'saas_trial_signups_namespace_marketing_vs'  AS metric_name,
        NULL                                         AS channel_grouping,
        COUNT(DISTINCT ultimate_parent_namespace_id) AS metric_value
    FROM rpt_namespace_onboarding
    JOIN namespaces_in_trial_touched_marketing_site
        ON rpt_namespace_onboarding.ultimate_parent_namespace_id = namespaces_in_trial_touched_marketing_site.gsc_namespace_id
    WHERE trial_start_date >= DATE_TRUNC(DAY, page_view_start_at)
        AND DATE_TRUNC('month', trial_start_date) = DATE_TRUNC('month', page_view_start_at)
        AND creator_is_valuable_signup
    GROUP BY 1,2,3

-- SaAS new User
), user_welcome_page AS ( 
    --welcome page can come after standard or SSO steps, so including requirement for all users in subsequent steps to view users/sign_up Page prior to, but ON the same day as subsequent steps. 

   SELECT DISTINCT
    snowplow_page_views_base.page_view_start_at,
    snowplow_page_views_base.user_snowplow_domain_id
  FROM snowplow_page_views_base
  JOIN snowplow_page_views_marketing
    ON snowplow_page_views_base.user_snowplow_domain_id = snowplow_page_views_marketing.user_snowplow_domain_id 
        AND snowplow_page_views_base.page_view_start_at::DATE = snowplow_page_views_marketing.first_marketing_page_view::DATE
        AND snowplow_page_views_base.page_view_start_at > snowplow_page_views_marketing.first_marketing_page_view
  WHERE snowplow_page_views_base.page_url LIKE '%users/sign_up/welcome'
    AND page_view_start_at >= '2022-01-01'  

-- we should be able to make is_valuable_signup a real field
), sass_new_user AS (

    SELECT 
        DATE_TRUNC(month, page_view_start_at)           AS metric_time,
        'saas_new_user'                              AS metric_name,
        NULL                                         AS channel_grouping,
        COUNT(DISTINCT user_snowplow_domain_id)      AS metric_value
    FROM user_welcome_page
    GROUP BY 1,2,3

), day_14_active_trials_rate AS (

    SELECT
        trials_base.*,
        DATEADD(DAY, 14, trial_start_date) AS day_14_of_trial
    FROM trials_base
    WHERE DATEADD(DAY, 45, trial_start_date) <= CURRENT_DATE
        AND creator_is_valuable_signup

), namespaces_14_day_activation_rate AS (

    SELECT
        DATE_TRUNC('month', day_14_of_trial)         AS metric_time,
        '14_day_activation_rate'                     AS metric_name,
        NULL                                         AS channel_grouping,
        COUNT(DISTINCT CASE WHEN has_team_activation THEN ultimate_parent_namespace_id END)
            / COUNT(DISTINCT ultimate_parent_namespace_id) AS metric_value
    FROM day_14_active_trials_rate
    WHERE metric_time >= '2022-01-01'
        AND creator_is_valuable_signup
    GROUP BY 1,2,3

), namespaces_14_day_activation_numerator AS (

    SELECT
        DATE_TRUNC('month', day_14_of_trial)         AS metric_time,
        '14_day_activation_numerator'                AS metric_name,
        NULL                                         AS channel_grouping,
        COUNT(DISTINCT CASE WHEN has_team_activatiON THEN ultimate_parent_namespace_id END) AS metric_value
    FROM day_14_active_trials_rate
    WHERE metric_time >= '2022-01-01'
    AND creator_is_valuable_signup
    GROUP BY 1,2,3

), namespaces_14_day_activation_denominator AS (

    SELECT
        DATE_TRUNC('month', day_14_of_trial)         AS metric_time,
        '14_day_activation_denominator'              AS metric_name,
        NULL                                         AS channel_grouping,
        COUNT(DISTINCT ultimate_parent_namespace_id) AS metric_value
    FROM day_14_active_trials_rate
    WHERE metric_time >= '2022-01-01'
        AND creator_is_valuable_signup
    GROUP BY 1,2,3    

-- 45-day free to paid conversion rate
), namespaces_on_day_45_of_trial AS (

    -- gets namespace ids that just started a trial based on the segment 
    SELECT  
        trials_base.*,
        DATEADD(DAY, 45, trial_start_date) AS day_45_of_trial
    FROM trials_base
        -- limmit to trials ON thier x date
    WHERE DATEADD(DAY, 45, trial_start_date) <= CURRENT_DATE
        AND creator_is_valuable_signup

), day_45_free_to_paid_rate AS (

    SELECT
        DATE_TRUNC('month', day_45_of_trial)               AS metric_time,
        '45_day_trial_paid_rate'                           AS metric_name,
        NULL                                               AS channel_grouping,
        COUNT(DISTINCT 
            CASE WHEN 
                -- subscribed within 45 days
                first_paid_subscription_start_date <= day_45_of_trial 
                -- ensure the paid sub IS after the trial
                AND first_paid_subscription_start_date >= trial_start_date 
                THEN ultimate_parent_namespace_id END)     AS metric_value
    FROM namespaces_on_day_45_of_trial
    WHERE metric_time >= '2022-01-01'  
        AND creator_is_valuable_signup
    GROUP BY 1,2,3

), day_45_free_to_paid_numerator AS (

    SELECT
        DATE_TRUNC('month', day_45_of_trial)               AS metric_time,
        '45_day_trial_paid_numerator'                      AS metric_name,
        NULL                                               AS channel_grouping,
        COUNT(DISTINCT 
            CASE WHEN 
                -- subscribed within 45 days
                first_paid_subscription_start_date <= day_45_of_trial 
                -- ensure the paid sub IS after the trial
                AND first_paid_subscription_start_date >= trial_start_date 
                THEN ultimate_parent_namespace_id END)     AS metric_value
    FROM namespaces_on_day_45_of_trial
    WHERE metric_time >= '2022-01-01'  
        AND creator_is_valuable_signup
    GROUP BY 1,2,3

),  day_45_free_to_paid_denominator AS (

    SELECT
        DATE_TRUNC('month', day_45_of_trial)               AS metric_time,
        '45_day_trial_paid_denominator'                    AS metric_name,
        NULL                                               AS channel_grouping,
        COUNT(DISTINCT ultimate_parent_namespace_id)       AS metric_value
    FROM namespaces_on_day_45_of_trial
    WHERE metric_time >= '2022-01-01'  
        AND creator_is_valuable_signup
    GROUP BY 1,2,3

),  paid_conversions_non_cohorted AS (

    -- gets namespace ids that just started a trial based ON the segment 
    SELECT
        DATE_TRUNC('month', first_paid_subscription_start_date) AS metric_time,
        'paid_conversions_non_cohorted'                         AS metric_name,
        NULL                                                    AS channel_grouping,
        COUNT(DISTINCT ultimate_parent_namespace_id)            AS metric_value
    FROM trials_base
    WHERE trial_start_date IS NOT null
        AND first_paid_subscription_start_date >= trial_start_date
        AND first_paid_subscription_start_date >= '2022-01-01'  
        AND creator_is_valuable_signup
    GROUP BY 1,2,3

), final_union AS (

    SELECT *
    FROM site_sessions
    UNION ALL
    SELECT *
    FROM site_sessions_exclude_signin
    UNION ALL
    SELECT *
    FROM new_users
    UNION ALL
    SELECT *
    FROM new_users_exclude_signin
    UNION ALL
    SELECT *
    FROM engaged_session
    UNION ALL
    SELECT *
    FROM engaged_session_exclude_signin
    UNION ALL
    SELECT *
    FROM ga_saas_trial_signup
    UNION ALL
    SELECT *
    FROM pages_per_session
    UNION ALL
    SELECT *
    FROM pages_per_session_exclude_signin
    UNION ALL
    SELECT *
    FROM pages_per_trial_start
    UNION ALL
    SELECT *
    FROM snowplow_page_views_trials
    UNION ALL
    SELECT *
    FROM snowplow_trial_signup_namespaces
    UNION ALL
    SELECT *
    FROM snowplow_trial_signup_namespaces_vs
    UNION ALL
    SELECT *
    FROM sass_new_user
    UNION ALL
    SELECT *
    FROM namespaces_14_day_activation_rate
    UNION ALL
    SELECT *
    FROM namespaces_14_day_activation_numerator
    UNION ALL
    SELECT *
    FROM namespaces_14_day_activation_denominator
    UNION ALL
    SELECT *
    FROM day_45_free_to_paid_rate
    UNION ALL
    SELECT *
    FROM day_45_free_to_paid_numerator
    UNION ALL
    SELECT *
    FROM day_45_free_to_paid_denominator
    UNION ALL
    SELECT *
    FROM paid_conversions_non_cohorted

), final AS (

    SELECT
        metric_time::DATE AS metric_time,
        metric_name,
        channel_grouping,
        metric_value,
        fiscal_quarter_name_fy,
        fiscal_quarter,
        fiscal_year
    FROM final_union
    JOIN dim_date 
        ON dim_date.date_actual = final_union.metric_time

)

{{ dbt_audit(
    cte_ref="final",
    created_by ="@rkohnke",
    updated_by ="@michellecooper",
    created_date="2024-03-01",
    updated_date="2024-05-03",
  ) }}