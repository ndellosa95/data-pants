{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_sales_funnel_target AS (

    SELECT DISTINCT kpi_name
    FROM {{ ref('wk_prep_sales_funnel_target') }}

), 

prep_sales_funnel_actual AS (

    SELECT DISTINCT sales_funnel_kpi_name
    FROM {{ ref('wk_fct_sales_funnel_actual') }}
),

unioned AS (

    SELECT kpi_name
    FROM prep_sales_funnel_target

    UNION

    SELECT sales_funnel_kpi_name
    FROM prep_sales_funnel_actual

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['kpi_name'])}} AS dim_sales_funnel_kpi_sk,
  kpi_name                                            AS sales_funnel_kpi_name
FROM unioned
