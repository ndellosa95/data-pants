{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH cdot_created_invoices  AS 

(

/* Determine all invoices that were posted via CDot or API automation e.g. auto-renewal, QSR */

SELECT
DATE(DATE_TRUNC('month', invoice_date)) AS invoice_month,
dim_invoice_id
FROM {{ ref('dim_invoice') }}
WHERE status = 'Posted'
AND(created_by_id = '2c92a0fd55822b4d015593ac264767f2' 
OR created_by_id = '2c92a0107bde3653017bf00cd8a86d5a')

),

manually_modified_invoices AS 

(

/* Determine all invoices that were posted via CDot or API automation e.g. auto-renewal, QSR which were manually updated */

SELECT DISTINCT
DATE(DATE_TRUNC('month', invoice_date)) AS invoice_month,
dim_invoice.dim_invoice_id
FROM {{ ref('dim_invoice') }}
LEFT JOIN {{ref('wk_finance_fct_invoice_item_adjustment')}} ON wk_finance_fct_invoice_item_adjustment.invoice_id = dim_invoice.dim_invoice_id
LEFT JOIN {{ref('wk_finance_fct_credit_balance_adjustment')}} ON wk_finance_fct_credit_balance_adjustment.invoice_id = dim_invoice.dim_invoice_id
LEFT JOIN {{ref('wk_finance_fct_refund_invoice_payment')}} ON wk_finance_fct_refund_invoice_payment.invoice_id = dim_invoice.dim_invoice_id
WHERE dim_invoice.status = 'Posted'
AND wk_finance_fct_invoice_item_adjustment.invoice_id IS NULL
AND wk_finance_fct_credit_balance_adjustment.invoice_id IS NULL
AND wk_finance_fct_refund_invoice_payment.invoice_id IS NULL
AND (created_by_id = '2c92a0fd55822b4d015593ac264767f2' OR created_by_id = '2c92a0107bde3653017bf00cd8a86d5a')),

cdot_invoices_manual_intervention_monthly AS 

(

/* Calculate the aggregates monthly */

SELECT
cdot_created_invoices.invoice_month,
COUNT(cdot_created_invoices.dim_invoice_id) AS count_all_cdot_invoices,
COUNT(cdot_created_invoices.dim_invoice_id) - COUNT(manually_modified_invoices.dim_invoice_id) AS count_cdot_modified_invoices,
ROUND((( count_cdot_modified_invoices / count_all_cdot_invoices) * 100),2) AS percentage_manually_modified_cdot_invoices 
FROM cdot_created_invoices  
LEFT JOIN manually_modified_invoices ON manually_modified_invoices.dim_invoice_id = cdot_created_invoices.dim_invoice_id
GROUP BY cdot_created_invoices.invoice_month
ORDER BY cdot_created_invoices.invoice_month

),

final AS

(

/* Add fiscal year and quarter */


SELECT
cdot_invoices_manual_intervention_monthly.invoice_month                              AS invoice_month,
dim_date.fiscal_year                                                                 AS fiscal_year,
dim_date.fiscal_quarter_name_fy                                                      AS fiscal_quarter,
cdot_invoices_manual_intervention_monthly.count_all_cdot_invoices                    AS count_all_cdot_invoices,
cdot_invoices_manual_intervention_monthly.count_cdot_modified_invoices               AS count_cdot_modified_invoices,
cdot_invoices_manual_intervention_monthly.percentage_manually_modified_cdot_invoices AS percentage_manually_modified_cdot_invoices
FROM cdot_invoices_manual_intervention_monthly
LEFT JOIN {{ ref('dim_date') }} ON dim_date.date_actual = cdot_invoices_manual_intervention_monthly.invoice_month

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-06",
updated_date="2024-04-16"
) }}
