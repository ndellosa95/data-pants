{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH invoice_amounts AS

(

/* Total billing amount monthly including tax */

SELECT
opportunity_invoice_payment_year_month AS billing_month,
invoice_amount_with_tax AS total_billing
FROM prod.restricted_safe_workspace_finance.wk_mart_booking_billing_ar_monthly
ORDER BY billing_month

),

payment_amounts AS

(

/* Total payment amount monthly */

SELECT
opportunity_invoice_payment_year_month AS payment_month,
payment_amount * -1 AS payments
FROM prod.restricted_safe_workspace_finance.wk_mart_booking_billing_ar_monthly
ORDER BY payment_month

),

overpayments AS

(

/* Total overpayment amount monthly */

SELECT
cba_date AS overpayment_date,
SUM(overpayment) * -1 AS overpayment
FROM prod.restricted_safe_workspace_finance.wk_mart_processed_cba_monthly
GROUP BY cba_date
ORDER BY cba_date

),

refund_amounts AS

(

/* Total refund amount monthly */

SELECT
refund_period AS refund_month,
refund_amount
FROM prod.restricted_safe_workspace_finance.WK_MART_PROCESSED_REFUNDS_MONTHLY
ORDER BY refund_month

),

iia_amounts AS

(

/* Total invoice item adjustment amounts */

SELECT
iia_date AS adjustment_month,
iia_charge_amount + iia_credit_amount AS adjustment_amount
FROM prod.restricted_safe_workspace_finance.wk_mart_processed_iia_monthly
ORDER BY adjustment_month

),

aging_bucket_1 AS

(

/* Historical aging bucket amounts */

SELECT 
accounting_period,
SUM(balance) AS ar_current
FROM prod.restricted_safe_workspace_finance.wk_mart_historical_balance_by_payment_terms_bucket_monthly
WHERE aging_bucket = '1 -- Current'
GROUP BY aging_bucket, accounting_period
ORDER BY accounting_period

),

aging_bucket_2 AS

(

/* Historical aging bucket amounts */

SELECT 
accounting_period,
SUM(balance) AS ar_1_30
FROM prod.restricted_safe_workspace_finance.wk_mart_historical_balance_by_payment_terms_bucket_monthly
WHERE aging_bucket = '2 -- 1 to 30 days past due'
GROUP BY aging_bucket, accounting_period
ORDER BY accounting_period

),

aging_bucket_3 AS

(

/* Historical aging bucket amounts */

SELECT 
accounting_period,
SUM(balance) AS ar_31_60
FROM prod.restricted_safe_workspace_finance.wk_mart_historical_balance_by_payment_terms_bucket_monthly
WHERE aging_bucket = '3 -- 31 to 60 days past due'
GROUP BY aging_bucket, accounting_period
ORDER BY accounting_period

),

aging_bucket_4 AS

(

/* Historical aging bucket amounts */

SELECT 
accounting_period,
SUM(balance) AS ar_61_90
FROM prod.restricted_safe_workspace_finance.wk_mart_historical_balance_by_payment_terms_bucket_monthly
WHERE aging_bucket = '4 -- 61 to 90 days past due'
GROUP BY aging_bucket, accounting_period
ORDER BY accounting_period

),

aging_bucket_5 AS

(

/* Historical aging bucket amounts */

SELECT 
accounting_period,
SUM(balance) AS ar_91_120
FROM prod.restricted_safe_workspace_finance.wk_mart_historical_balance_by_payment_terms_bucket_monthly
WHERE aging_bucket = '5 -- 91 to 120 days past due'
GROUP BY aging_bucket, accounting_period
ORDER BY accounting_period

),

aging_bucket_6 AS

(

/* Historical aging bucket amounts */

SELECT 
accounting_period,
SUM(balance) AS ar_more_than_120
FROM prod.restricted_safe_workspace_finance.wk_mart_historical_balance_by_payment_terms_bucket_monthly
WHERE aging_bucket = '6 -- More than 120 days past due'
GROUP BY aging_bucket, accounting_period
ORDER BY accounting_period

),

aging_bucket_total AS

(

/* Historical aging bucket amounts */

SELECT 
accounting_period,
SUM(balance) AS total_ar
FROM prod.restricted_safe_workspace_finance.wk_mart_historical_balance_by_payment_terms_bucket_monthly
GROUP BY accounting_period
ORDER BY accounting_period

),

credit_balance AS

(

/* Credit balance amount monthly */

SELECT
cba_date,
ROUND(credit_balance_per_month,2) * -1 AS credit_balance
FROM prod.restricted_safe_workspace_finance.wk_mart_processed_cba_monthly
ORDER BY cba_date

),

future_dated_refunds_payments AS

(

/* Future date refunds and payments monthly */

SELECT
payment_month,
payments_refunds_against_future_invoices * -1 AS payments_refunds_against_future_invoices
FROM prod.restricted_safe_workspace_finance.wk_mart_payments_refunds_against_future_invoices
ORDER BY payment_month

),

table_basis AS

(

/* Joining all data by period monthly */

SELECT
invoice_amounts.billing_month AS period,
COALESCE(invoice_amounts.total_billing,0) AS total_billing,
COALESCE(payment_amounts.payments,0) AS payments,
COALESCE(payment_amounts.payments,0) - COALESCE(overpayments.overpayment,0) AS payment_without_overpayment,
COALESCE(overpayments.overpayment,0) AS overpayments,
COALESCE(refund_amounts.refund_amount,0) AS refund_amounts,
COALESCE(iia_amounts.adjustment_amount,0) AS adjustment_amount,
COALESCE(aging_bucket_1.ar_current,0) AS ar_current,
COALESCE(aging_bucket_2.ar_1_30,0) AS ar_1_30,
COALESCE(aging_bucket_3.ar_31_60,0) AS ar_31_60,
COALESCE(aging_bucket_4.ar_61_90,0) AS ar_61_90,
COALESCE(aging_bucket_5.ar_91_120,0) AS ar_91_120,
COALESCE(aging_bucket_6.ar_more_than_120,0) AS ar_more_than_120,
COALESCE(aging_bucket_total.total_ar,0) AS total_ar,
COALESCE(credit_balance.credit_balance,0) AS credit_balance,
COALESCE(future_dated_refunds_payments.PAYMENTS_REFUNDS_AGAINST_FUTURE_INVOICES,0) AS PAYMENTS_REFUNDS_AGAINST_FUTURE_INVOICES
FROM invoice_amounts
FULL OUTER JOIN payment_amounts ON payment_amounts.payment_month = period
FULL OUTER JOIN overpayments ON overpayments.overpayment_date = period
FULL OUTER JOIN refund_amounts ON refund_amounts.refund_month = period
FULL OUTER JOIN iia_amounts ON iia_amounts.adjustment_month = period
FULL OUTER JOIN aging_bucket_1 ON aging_bucket_1.accounting_period = period
FULL OUTER JOIN aging_bucket_2 ON aging_bucket_2.accounting_period = period
FULL OUTER JOIN aging_bucket_3 ON aging_bucket_3.accounting_period = period
FULL OUTER JOIN aging_bucket_4 ON aging_bucket_4.accounting_period = period
FULL OUTER JOIN aging_bucket_5 ON aging_bucket_5.accounting_period = period
FULL OUTER JOIN aging_bucket_6 ON aging_bucket_6.accounting_period = period
FULL OUTER JOIN aging_bucket_total ON aging_bucket_total.accounting_period = period
FULL OUTER JOIN credit_balance ON credit_balance.cba_date = period
FULL OUTER JOIN future_dated_refunds_payments ON future_dated_refunds_payments.payment_month = period
WHERE total_billing > 0
ORDER BY period

),

ending_ar AS

(

/* Calculating the ending account receivable */

SELECT *,
SUM(table_basis.total_billing + table_basis.refund_amounts + table_basis.adjustment_amount + table_basis.payments) OVER (ORDER BY table_basis.period) AS ending_accounts_receivable --running total of the relevant transactions per month
FROM table_basis

),

final AS

(

SELECT
dim_date.fiscal_year,
dim_date.fiscal_quarter_name,
ending_ar.period,
COALESCE(LAG(ending_ar.ending_accounts_receivable) OVER (ORDER BY ending_ar.period),0) AS starting_accounts_receivable, --starting AR equals to the ending AR from the previous month
ending_ar.total_billing,
ending_ar.payment_without_overpayment,
ending_ar.overpayments,
ending_ar.refund_amounts,
ending_ar.adjustment_amount,
ending_ar.ending_accounts_receivable,
ending_ar.ar_current,
ending_ar.ar_1_30,
ending_ar.ar_31_60,
ending_ar.ar_61_90,
ending_ar.ar_91_120,
ending_ar.ar_more_than_120,
ending_ar.total_ar AS total_invoice_aging_balance,
COALESCE(LAG(total_invoice_aging_balance) OVER (ORDER BY ending_ar.period),0) AS starting_total_invoice_aging_balance, --starting AR equals to the ending AR from the previous month
ending_ar.credit_balance,
ending_ar.PAYMENTS_REFUNDS_AGAINST_FUTURE_INVOICES
FROM ending_ar
LEFT JOIN prod.common.dim_date ON dim_date.date_actual = period

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-04-10",
updated_date="2024-04-10"
) }}

