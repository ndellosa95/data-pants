{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH final AS

(

/* View of historical balances with payment terms and aging bucket by month */

SELECT 
    wk_finance_fct_invoice_aging_detail.account_balance_impact                                AS balance,
    DATEDIFF(day, dim_invoice.invoice_date,dim_invoice.due_date)                              AS payment_terms,
    DATE(DATE_TRUNC('month', wk_finance_fct_invoice_aging_detail.accounting_period_end_date)) AS accounting_period,
    dim_date.fiscal_year                                                                      AS fiscal_year,
    dim_date.fiscal_quarter_name_fy                                                           AS fiscal_quarter,
    CASE 
    WHEN days_overdue <= 0 
    THEN '1 -- Current'
    WHEN days_overdue >= 1 AND days_overdue <= 30 
    THEN '2 -- 1 to 30 days past due'
    WHEN days_overdue >= 31 AND days_overdue <= 60 
    THEN '3 -- 31 to 60 days past due'
    WHEN days_overdue >= 61 AND days_overdue <= 90 
    THEN '4 -- 61 to 90 days past due'
    WHEN days_overdue >= 91 AND days_overdue <= 120 
    THEN '5 -- 91 to 120 days past due'
    WHEN days_overdue > 120
    THEN '6 -- More than 120 days past due'
    ELSE 'n/a'
    END AS aging_bucket
    FROM {{ ref('wk_finance_fct_invoice_aging_detail') }}
    LEFT JOIN {{ ref('dim_invoice') }} ON dim_invoice.dim_invoice_id = wk_finance_fct_invoice_aging_detail.invoice_id
    LEFT JOIN {{ ref('dim_date') }} ON dim_date.date_actual = accounting_period

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-28",
updated_date="2024-04-15"
) }}

