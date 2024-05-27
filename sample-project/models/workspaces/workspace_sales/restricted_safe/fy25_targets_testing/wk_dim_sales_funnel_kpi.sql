{{ config(
    tags=["mnpi_exception"]
) }}

SELECT *
FROM {{ ref('wk_prep_sales_funnel_kpi') }}
