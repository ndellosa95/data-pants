with cost_data as (

    SELECT * FROM {{ ref('rpt_gcp_billing_pl_day_ext')}}
    where date_day >= '2023-02-01'

),

gitlab_data_ AS (

SELECT * FROM {{ ref('ci_runners_pl_daily')}}

),

cloud_data as (

SELECT date_day,
    gcp_sku_description,
    level_3,
    level_4,
    CASE 
    WHEN (level_3 = 'SaaS' and level_4 = 'linux small ') then '2 - shared saas runners - small'
    WHEN (level_3 = 'SaaS' and level_4 = 'linux medium') then '3 - shared saas runners - medium'
    WHEN (level_3 = 'SaaS' and level_4 = 'linux large') then '4 - shared saas runners - large'
    WHEN (level_3 = 'SaaS' and level_4 = 'linux xlarge') then '10 - shared saas runners - xlarge'
    WHEN (level_3 = 'SaaS' and level_4 = 'linux 2xlarge') then '11 - shared saas runners - 2xlarge'
    WHEN (level_3 = 'SaaS' and level_4 = 'linux medium gpu') then '8 - shared saas runners gpu - medium'
    WHEN (level_3 = 'Internal' and level_4 = 'linux private internal') then '6 - private internal runners'
    WHEN (level_3 = 'Shared org' and level_4 = 'linux small ') then '1 - shared gitlab org runners'
    END AS mapping,
    sum(usage_amount_in_pricing_units)*60 as usage_amount_in_pricing_units,
    sum(net_cost) as net_cost
FROM cost_data
where level_4 IN ('linux small ', 'linux medium', 'linux large', 'linux xlarge', 'linux 2xlarge', 'linux medium gpu', 'linux private internal')
and (gcp_sku_description like 'N2D AMD Instance Core running in%'
or gcp_sku_description like 'N1 Predefined Instance Core running%')
group by 1,2,3,4
order by 1 desc

),

gitlab_data as (

    SELECT reporting_day as date_day,
    mapping,
    sum(total_ci_minutes) as total_ci_minutes
    FROM gitlab_data_
    WHERE reporting_day >= '2023-02-01'
    group by 1,2

),

joined as (

    SELECT c.date_day,
    c.level_3,
    c.level_4,
    c.usage_amount_in_pricing_units as cloud_compute_minutes,
    c.net_cost,
    g.total_ci_minutes as gitlab_ci_minutes,
    c.usage_amount_in_pricing_units - g.total_ci_minutes  as overhead_compute_minutes,
    c.usage_amount_in_pricing_units/g.total_ci_minutes as compute_efficiency,
    c.net_cost/(g.total_ci_minutes/1000) as dollar_efficency_cost_for_1000_ci_minutes
    FROM cloud_data c
    LEFT JOIN gitlab_data g
    ON c.date_day = g.date_day
    AND c.mapping = g.mapping

)

SELECT * FROM joined
order by 1 desc