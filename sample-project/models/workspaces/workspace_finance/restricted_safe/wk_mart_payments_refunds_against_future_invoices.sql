{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH invoice_payment AS

(

/* Payments applied to invoices joining invoice information */

SELECT
DATE(DATE_TRUNC('month', dim_invoice.invoice_date))   AS invoice_month,
wk_finance_fct_invoice_payment.invoice_payment_amount AS applied_payment_amount,
wk_finance_fct_invoice_payment.invoice_id,
wk_finance_fct_invoice_payment.payment_id
FROM {{ ref('wk_finance_fct_invoice_payment') }}
LEFT JOIN prod.restricted_safe_common.fct_invoice ON fct_invoice.dim_invoice_id = wk_finance_fct_invoice_payment.invoice_id
LEFT JOIN prod.common.dim_invoice ON dim_invoice.dim_invoice_id = fct_invoice.dim_invoice_id

),

payment AS

(

/* Select all payments to determine the payment date */

SELECT
DATE(DATE_TRUNC('month', wk_finance_fct_payment.payment_date)) AS payment_month,
wk_finance_fct_payment.payment_amount                          AS payment_amount,
wk_finance_fct_payment.payment_id 
FROM {{ ref('wk_finance_fct_payment') }}

),

invoice_payment_and_payment AS

(

/* Determine payments that were applied to invoices with future dates */

SELECT
payment.payment_month,
payment.payment_amount
FROM invoice_payment
JOIN payment ON payment.payment_id = invoice_payment.payment_id
WHERE invoice_payment.invoice_month > payment.payment_month

),

final AS

(

SELECT
invoice_payment_and_payment.payment_month,
SUM(invoice_payment_and_payment.payment_amount) AS payments_refunds_against_future_invoices
FROM invoice_payment_and_payment
GROUP BY invoice_payment_and_payment.payment_month
ORDER BY invoice_payment_and_payment.payment_month ASC

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-26",
updated_date="2024-03-26"
) }}

