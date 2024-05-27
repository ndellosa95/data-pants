{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH booking_billing AS

(

/* adding booking and billing aggregated amounts per month */

SELECT
wk_mart_booking_billing_ar_monthly.opportunity_invoice_payment_year_month,
ROUND(wk_mart_booking_billing_ar_monthly.booking_amount,2)                                                                   AS booking_amount,
fiscal_year,
fiscal_quarter,
ROUND(wk_mart_booking_billing_ar_monthly.invoice_amount_without_tax,2)                                                       AS invoice_amount_without_tax,
ROUND(wk_mart_booking_billing_ar_monthly.invoice_tax_amount,2)                                                               AS invoice_tax_amount,
ROUND(wk_mart_booking_billing_ar_monthly.invoice_amount_with_tax,2)                                                          AS invoice_amount_with_tax,
ROUND((wk_mart_booking_billing_ar_monthly.booking_amount - wk_mart_booking_billing_ar_monthly.invoice_amount_without_tax),2) AS variance_booking_billing
FROM prod.restricted_safe_workspace_finance.wk_mart_booking_billing_ar_monthly
ORDER BY wk_mart_booking_billing_ar_monthly.opportunity_invoice_payment_year_month

),

collections AS

(

/* adding payments collected for given invoice month */

SELECT
wk_mart_collections_monthly.billed_month,
SUM(wk_mart_collections_monthly.payment_applied_to_invoice) AS total_current_collected
FROM prod.restricted_safe_workspace_finance.wk_mart_collections_monthly
GROUP BY wk_mart_collections_monthly.billed_month
ORDER BY wk_mart_collections_monthly.billed_month

),

final AS

(

/* adding booking, billing and AR data by month */

SELECT 
booking_billing.opportunity_invoice_payment_year_month,
booking_billing.fiscal_year,
booking_billing.fiscal_quarter,
booking_billing.booking_amount,
booking_billing.invoice_amount_without_tax,
booking_billing.invoice_tax_amount,
booking_billing.invoice_amount_with_tax,
booking_billing.variance_booking_billing,
collections.total_current_collected,
ROUND(((collections.total_current_collected/booking_billing.invoice_amount_with_tax)*100),2) AS percentage_collected_in_period,
ROUND((100 - percentage_collected_in_period),2)                                              AS percentage_out_of_period
FROM booking_billing
LEFT JOIN collections ON collections.billed_month = booking_billing.opportunity_invoice_payment_year_month

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-29",
updated_date="2024-04-08"
) }}

