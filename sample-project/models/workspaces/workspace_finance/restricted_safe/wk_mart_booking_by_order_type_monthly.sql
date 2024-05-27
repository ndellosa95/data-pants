{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH basis AS

(

/* Booking amount and count by sales type */

SELECT
DATE(DATE_TRUNC('month',fct_crm_opportunity.close_date)) AS booking_month,
dim_crm_opportunity.sales_type                           AS sales_type,
SUM(fct_crm_opportunity.amount)                          AS opportunity_amount,
COUNT(fct_crm_opportunity.amount)                        AS opportunity_count
FROM {{ ref('fct_crm_opportunity') }}
LEFT JOIN {{ ref('dim_crm_opportunity') }} ON dim_crm_opportunity.dim_crm_opportunity_id = fct_crm_opportunity.dim_crm_opportunity_id
WHERE dim_crm_opportunity.is_won = TRUE
GROUP BY booking_month, dim_crm_opportunity.sales_type
ORDER BY booking_month, dim_crm_opportunity.sales_type

),

final AS

(

/* Adding fiscal year and quarter */

SELECT
basis.booking_month               AS booking_month,
dim_date.fiscal_year              AS fiscal_year,
dim_date.fiscal_quarter_name_fy   AS fiscal_quarter,
basis.sales_type                  AS sales_type,
basis.opportunity_amount          AS opportunity_amount,
basis.opportunity_count           AS opportunity_count
FROM basis
LEFT JOIN {{ ref('dim_date') }} ON dim_date.date_actual = basis.booking_month

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-27",
updated_date="2024-04-15"
) }}

