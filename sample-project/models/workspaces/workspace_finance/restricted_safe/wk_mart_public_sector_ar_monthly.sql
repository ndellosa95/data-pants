{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH segment AS (

/* Determine purchase segment of open invoices monthly */

SELECT DISTINCT
wk_finance_fct_invoice_aging_detail.invoice_id,
DATE(DATE_TRUNC('month',wk_finance_fct_invoice_aging_detail.accounting_period_end_date)) AS accounting_period_month,
wk_finance_fct_invoice_aging_detail.account_balance_impact,
CASE
WHEN dim_crm_opportunity.user_segment = 'PUBSEC'
THEN 'PubSec'
ELSE 'Non-PubSec'
END AS segment
FROM {{ ref('wk_finance_fct_invoice_aging_detail') }}
LEFT JOIN {{ ref('dim_invoice') }} ON dim_invoice.dim_invoice_id = wk_finance_fct_invoice_aging_detail.invoice_id
LEFT JOIN {{ ref('dim_crm_opportunity') }} ON dim_crm_opportunity.invoice_number = dim_invoice.invoice_number

),

balance_per_segment AS (

/* Determine the total balances per purchase segment monthly */

SELECT
segment.accounting_period_month,
segment.segment,
SUM(segment.account_balance_impact)   AS total_balance_per_segment,
COUNT(segment.account_balance_impact) AS invoice_count_per_segment
FROM segment
GROUP BY segment.accounting_period_month, segment.segment
ORDER BY segment.accounting_period_month, segment.segment

),

total AS (

/* Determine the total balances for all open invoices monthly */

SELECT
DATE(DATE_TRUNC('month',wk_finance_fct_invoice_aging_detail.accounting_period_end_date)) AS accounting_period_month,
SUM(account_balance_impact)                                                              AS total_all_balance,
COUNT(account_balance_impact)                                                            AS count_all_open_invoices
FROM {{ ref('wk_finance_fct_invoice_aging_detail') }}
GROUP BY accounting_period_month

),

final AS (

/* Compare balances and count per segment vs. the total balances for all open invoices monthly */

SELECT
balance_per_segment.accounting_period_month                                                    AS accounting_period_month,
dim_date.fiscal_year                                                                           AS fiscal_year,
dim_date.fiscal_quarter_name_fy                                                                AS fiscal_quarter,
balance_per_segment.segment                                                                    AS segment,
ROUND((balance_per_segment.total_balance_per_segment / total.total_all_balance) * 100,2)       AS percentage_of_open_balance_per_segment,
balance_per_segment.total_balance_per_segment                                                  AS total_balance_per_segment,
ROUND((balance_per_segment.invoice_count_per_segment / total.count_all_open_invoices) * 100,2) AS percentage_of_open_invoices_count_per_segment,
balance_per_segment.invoice_count_per_segment                                                  AS invoice_count_per_segment
FROM balance_per_segment
LEFT JOIN total ON total.accounting_period_month = balance_per_segment.accounting_period_month
LEFT JOIN {{ ref('dim_date') }} ON dim_date.date_actual = balance_per_segment.accounting_period_month

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-24",
updated_date="2024-04-16"
) }}

