{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH purchase_path  AS (

/* Determine purchase path of open invoices monthly */

SELECT
wk_finance_fct_invoice_aging_detail.invoice_id,
wk_finance_fct_invoice_aging_detail.accounting_period_end_date,
wk_finance_fct_invoice_aging_detail.account_balance_impact,
CASE
WHEN dim_invoice.created_by_id = '2c92a0fd55822b4d015593ac264767f2'
THEN 'CDot'
WHEN dim_invoice.created_by_id = '2c92a0107bde3653017bf00cd8a86d5a'
THEN 'CDot'
ELSE 'Sales Assisted'
END AS purchase_path
FROM {{ ref('wk_finance_fct_invoice_aging_detail') }}
LEFT JOIN {{ ref('dim_invoice') }} ON dim_invoice.dim_invoice_id = wk_finance_fct_invoice_aging_detail.invoice_id

),

balance_per_purchase_path AS (

/* Determine the total balances per purchase path monthly */

SELECT
purchase_path.accounting_period_end_date,
purchase_path.purchase_path,
SUM( purchase_path.account_balance_impact) AS total_balance_per_path,
COUNT( purchase_path.account_balance_impact) AS invoice_count_per_path
FROM  purchase_path
GROUP BY purchase_path.accounting_period_end_date,  purchase_path.purchase_path
ORDER BY purchase_path.accounting_period_end_date,  purchase_path.purchase_path

),

total AS (

/* Determine the total balances for all open invoices monthly */

SELECT
accounting_period_end_date AS accounting_period,
SUM(account_balance_impact) AS total_all_balance,
COUNT(account_balance_impact) AS count_all_open_invoices
FROM {{ ref('wk_finance_fct_invoice_aging_detail') }}
GROUP BY accounting_period

),

final AS (

/* Compare balances and count per path vs. the total balances for all open invoices monthly */

SELECT
DATE(DATE_TRUNC('month',balance_per_purchase_path.accounting_period_end_date))                    AS accounting_period_end_date,
dim_date.fiscal_year                                                                              AS fiscal_year,
dim_date.fiscal_quarter_name_fy                                                                   AS fiscal_quarter,
balance_per_purchase_path.purchase_path,
ROUND((balance_per_purchase_path.total_balance_per_path / total.total_all_balance) * 100,2)       AS percentage_of_open_balance_per_path,
balance_per_purchase_path.total_balance_per_path,
ROUND((balance_per_purchase_path.invoice_count_per_path / total.count_all_open_invoices) * 100,2) AS percentage_of_open_invoices_count_per_path,
balance_per_purchase_path.invoice_count_per_path
FROM balance_per_purchase_path
LEFT JOIN total ON total.accounting_period = balance_per_purchase_path.accounting_period_end_date
LEFT JOIN {{ ref('dim_date') }} ON dim_date.date_actual = balance_per_purchase_path.accounting_period_end_date

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-07",
updated_date="2024-04-15"
) }}

