{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH opportunity_data AS (

/* Table providing opportunity amounts */

SELECT
DATE_TRUNC('month', close_date) AS opportunity_close_month,
SUM(amount)                     AS booking_amount,
COUNT(amount)                   AS booking_count
FROM {{ ref('fct_crm_opportunity') }}
WHERE is_closed_won = TRUE
GROUP BY opportunity_close_month

),

invoice_data AS (

/* Table providing invoice amounts */

SELECT
DATE(DATE_TRUNC('month', dim_invoice.invoice_date))           AS invoice_month,
SUM(fct_invoice.amount)                                       AS invoice_amount_with_tax,
SUM(fct_invoice.amount_without_tax)                           AS invoice_amount_without_tax,
SUM(fct_invoice.amount) - SUM(fct_invoice.amount_without_tax) AS invoice_tax_amount,
COUNT(fct_invoice.amount)                                     AS invoice_count
FROM {{ ref('dim_invoice') }}
JOIN {{ ref('fct_invoice') }} ON fct_invoice.dim_invoice_id = dim_invoice.dim_invoice_id
WHERE dim_invoice.status = 'Posted'
GROUP BY invoice_month

),

payment_data AS (

/* Table providing payment amounts */

SELECT
DATE(DATE_TRUNC('month', payment_date)) AS payment_month,
SUM(payment_amount)                     AS payment_amount,
COUNT(payment_amount)                   AS payment_count
FROM {{ ref('wk_finance_fct_payment') }}
WHERE payment_status = 'Processed'
GROUP BY payment_month

),

final AS (

SELECT
dim_date.fiscal_year                                 AS fiscal_year,
dim_date.fiscal_quarter_name_fy                      AS fiscal_quarter,
opportunity_data.opportunity_close_month             AS opportunity_invoice_payment_year_month,
COALESCE(opportunity_data.booking_amount, 0)         AS booking_amount,
COALESCE(opportunity_data.booking_count, 0)          AS booking_count,
COALESCE(invoice_data.invoice_amount_with_tax, 0)    AS invoice_amount_with_tax,
COALESCE(invoice_data.invoice_amount_without_tax, 0) AS invoice_amount_without_tax,
COALESCE(invoice_data.invoice_tax_amount, 0)         AS invoice_tax_amount,
COALESCE(invoice_data.invoice_count, 0)              AS invoice_count,
COALESCE(payment_data.payment_amount, 0)             AS payment_amount,
COALESCE(payment_data.payment_count, 0)              AS payment_count
FROM opportunity_data 
LEFT JOIN invoice_data ON invoice_data.invoice_month  = opportunity_data.opportunity_close_month
LEFT JOIN payment_data ON payment_data.payment_month = opportunity_data.opportunity_close_month
LEFT JOIN {{ ref('dim_date') }} ON dim_date.date_actual = opportunity_invoice_payment_year_month
ORDER BY opportunity_invoice_payment_year_month

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-07",
updated_date="2024-04-08"
) }}

