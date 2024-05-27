
{{ config(
    materialized='table',
    )
}}

with cloud_data as (

    SELECT date_day,
    usage_standard_unit,
    sum(usage_amount_in_standard_unit) as usage_amount_in_standard_units,
    sum(net_cost) as net_cost
    FROM {{ ref ('rpt_gcp_billing_pl_day_ext')}}
    where level_2 = 'Repository Storage'
    and gcp_sku_description IN ('SSD backed PD Capacity', 'Storage PD Snapshot in US', 'Balanced PD Capacity', 'Storage PD Capacity')
    and date_day >= '2023-02-01'
    group by 1,2
    order by 1 desc

),

gitlab_data as (

    SELECT snapshot_day as date_day,
    sum(repo_size_gb) as gitlab_repo_size_gb
    FROM {{ ref ('repo_storage_pl_daily')}}
    WHERE snapshot_day >= '2023-02-01'
    group by 1

),

joined as (

    SELECT c.date_day,
    c.usage_standard_unit,
    c.usage_amount_in_standard_units,
    c.net_cost,
    g.gitlab_repo_size_gb,
    c.usage_amount_in_standard_units - g.gitlab_repo_size_gb as overhead_gb,
    c.usage_amount_in_standard_units/g.gitlab_repo_size_gb as overhead_percent,
    c.net_cost/(g.gitlab_repo_size_gb/1024) as monthly_unit_price_per_repo_tb
    FROM cloud_data c
    LEFT JOIN gitlab_data g
    ON c.date_day = g.date_day

)

SELECT * FROM joined
order by 1 desc