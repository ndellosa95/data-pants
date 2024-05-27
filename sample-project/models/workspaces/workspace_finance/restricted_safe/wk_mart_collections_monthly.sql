{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH invoice_detail AS (

/* Selecting the applied payment amount with status ‘processed’, invoice month and payment month */

SELECT
DATE(DATE_TRUNC('month', dim_invoice.invoice_date)) AS invoice_month,
DATE(DATE_TRUNC('month', wk_finance_fct_payment.payment_date)) AS payment_month,
SUM(wk_finance_fct_invoice_payment.invoice_payment_amount) AS payment_applied_to_invoice 
FROM {{ ref('wk_finance_fct_invoice_payment') }}
LEFT JOIN {{ ref('dim_invoice') }}  ON dim_invoice.dim_invoice_id = wk_finance_fct_invoice_payment.invoice_id
LEFT JOIN {{ ref('wk_finance_fct_payment') }}  ON wk_finance_fct_payment.payment_id = wk_finance_fct_invoice_payment.payment_id
WHERE wk_finance_fct_payment.payment_status = 'Processed'
GROUP BY payment_month, invoice_month
ORDER BY payment_month, invoice_month

),

total_billed AS (

/* Total billed per month */

SELECT
DATE(DATE_TRUNC('month', dim_invoice.invoice_date)) AS billed_month,
SUM(fct_invoice.amount) AS total_billed,
FROM {{ ref('dim_invoice') }}
JOIN {{ ref('fct_invoice') }} ON fct_invoice.dim_invoice_id = dim_invoice.dim_invoice_id
WHERE dim_invoice.status = 'Posted'
GROUP BY billed_month

),

final AS (

/* Combined the applied payment with billed data per month */

SELECT
dim_date.fiscal_year AS payment_period_FY,
dim_date.fiscal_quarter_name AS payment_period_FQ,
invoice_detail.payment_month,
invoice_detail.payment_applied_to_invoice,
total_billed.billed_month,
total_billed.total_billed,
ROUND(((invoice_detail.payment_applied_to_invoice / total_billed.total_billed) * 100), 2) AS percentage_payment_applied_to_billed,
DATEDIFF(month, total_billed.billed_month, invoice_detail.payment_month) AS period
FROM invoice_detail
LEFT JOIN total_billed ON total_billed.billed_month = invoice_detail.invoice_month
LEFT JOIN {{ ref('dim_date') }} ON dim_date.date_actual = invoice_detail.payment_month

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-07",
updated_date="2024-03-07"
) }}

