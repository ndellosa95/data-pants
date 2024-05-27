{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH ar_basis AS

(

/* Adding payment terms and days overdue to invoices */

SELECT
DATE(DATE_TRUNC('month', dim_invoice.invoice_date))          AS invoice_period,
DATEDIFF(day, dim_invoice.due_date, CURRENT_DATE())          AS days_overdue,
DATEDIFF(day, dim_invoice.invoice_date,dim_invoice.due_date) AS payment_terms,
fct_invoice.balance                                          AS balance,
fct_invoice.amount                                           AS invoice_amount
FROM {{ ref('dim_invoice') }}
LEFT JOIN {{ ref('fct_invoice') }} ON fct_invoice.dim_invoice_id = dim_invoice.dim_invoice_id

),

final AS

(

/* Adding aging bucket to invoices that were ever overdue */

SELECT
invoice_period,
payment_terms,
days_overdue,
balance,
invoice_amount,
CASE 
    WHEN days_overdue <= 0 AND balance <> 0
    THEN '1 -- Current'
    WHEN (days_overdue >= 1 AND days_overdue <= 30) AND balance <> 0 
    THEN '2 -- 1 to 30 days past due'
    WHEN (days_overdue >= 31 AND days_overdue <= 60) AND balance <> 0 
    THEN '3 -- 31 to 60 days past due'
    WHEN (days_overdue >= 61 AND days_overdue <= 90) AND balance <> 0
    THEN '4 -- 61 to 90 days past due'
    WHEN (days_overdue >= 91 AND days_overdue <= 120) AND balance <> 0
    THEN '5 -- 91 to 120 days past due'
    WHEN days_overdue > 120 AND balance <> 0
    THEN '6 -- More than 120 days past due'
    ELSE 'n/a'
    END AS aging_bucket
FROM ar_basis
ORDER BY invoice_period, aging_bucket

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-04-15",
updated_date="2024-04-15"
) }}

