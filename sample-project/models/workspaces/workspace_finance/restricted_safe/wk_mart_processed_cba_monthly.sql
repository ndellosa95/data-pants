{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH increase_cba AS

(

SELECT 
DATE(DATE_TRUNC('month', wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_date)) AS cba_increase_date,
SUM(wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_amount) AS increase
FROM prod.restricted_safe_workspace_finance.wk_finance_fct_credit_balance_adjustment
WHERE wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_type = 'Increase'
AND wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_status = 'Processed'
GROUP BY cba_increase_date
ORDER BY cba_increase_date

),

decrease_cba AS

(

SELECT
DATE(DATE_TRUNC('month', wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_date)) AS cba_decrease_date,
SUM(wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_amount) AS decrease
FROM prod.restricted_safe_workspace_finance.wk_finance_fct_credit_balance_adjustment
WHERE wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_type = 'Decrease'
AND wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_status = 'Processed'
GROUP BY cba_decrease_date
ORDER BY cba_decrease_date

),

overpayment AS

(

SELECT 
DATE(DATE_TRUNC('month', wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_date)) AS overpayment_date,
SUM(wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_amount) AS overpayment
FROM prod.restricted_safe_workspace_finance.wk_finance_fct_credit_balance_adjustment
WHERE wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_type = 'Increase'
AND wk_finance_fct_credit_balance_adjustment.credit_balance_adjustment_status = 'Processed'
AND wk_finance_fct_credit_balance_adjustment.invoice_id = ''
GROUP BY overpayment_date
ORDER BY overpayment_date

),

final AS

(

SELECT 
COALESCE(increase_cba.cba_increase_date, decrease_cba.cba_decrease_date) AS cba_date,
COALESCE(increase_cba.increase,0) AS increase,
COALESCE(decrease_cba.decrease,0) AS decrease,
COALESCE(overpayment.overpayment,0) AS overpayment,
SUM(increase_cba.increase - decrease_cba.decrease) OVER (ORDER BY cba_date) AS credit_balance_per_month
FROM increase_cba
FULL OUTER JOIN decrease_cba ON decrease_cba.cba_decrease_date = increase_cba.cba_increase_date
FULL OUTER JOIN overpayment ON overpayment.overpayment_date = increase_cba.cba_increase_date
ORDER BY cba_date

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-29",
updated_date="2024-04-08"
) }}

