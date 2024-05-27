{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH payment_by_type AS (

/* Table providing payment amounts and volumes by type, electronic or external, per month */

SELECT
DATE(DATE_TRUNC('month', wk_finance_fct_payment.payment_date)) AS payment_month,
wk_finance_fct_payment.payment_type AS payment_type,
SUM(wk_finance_fct_payment.payment_amount) AS payment_amount_by_type,
COUNT(wk_finance_fct_payment.payment_amount) AS number_of_payments_by_type
FROM  {{ ref('wk_finance_fct_payment') }}
GROUP BY payment_month, payment_type
ORDER BY payment_month, payment_type

),

all_payments AS (

/* Table providing total payment amounts and volumes per month */

SELECT
DATE(DATE_TRUNC('month', wk_finance_fct_payment.payment_date)) AS payment_month,
SUM(wk_finance_fct_payment.payment_amount) AS total_payment_amount,
COUNT(wk_finance_fct_payment.payment_amount) AS total_number_of_payments
FROM  {{ ref('wk_finance_fct_payment') }}
GROUP BY payment_month
ORDER BY payment_month

),

final AS (

/* Table comparing total payment amounts and volumes per type per month */

SELECT
payment_by_type.payment_month                                                                        AS payment_month,
dim_date.fiscal_year                                                                                 AS fiscal_year,
dim_date.fiscal_quarter_name_fy                                                                      AS fiscal_quarter,
payment_by_type.payment_type                                                                         AS payment_by_type,
payment_by_type.payment_amount_by_type                                                               AS payment_amount_by_type, 
all_payments.total_payment_amount                                                                    AS total_payment_amount,
ROUND((( payment_by_type.payment_amount_by_type / all_payments.total_payment_amount)*100),2)         AS percentage_payment_by_type_amount,
payment_by_type.number_of_payments_by_type                                                           AS number_of_payments_by_type,
all_payments.total_number_of_payments                                                                AS total_number_of_payments,
ROUND((( payment_by_type.number_of_payments_by_type / all_payments.total_number_of_payments)*100),2) AS percentage_payment_by_type_count
FROM payment_by_type
LEFT JOIN all_payments ON all_payments.payment_month = payment_by_type.payment_month
LEFT JOIN {{ ref('dim_date') }} ON dim_date.date_actual = payment_by_type.payment_month
ORDER BY payment_by_type.payment_month, payment_by_type.payment_type

)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-08",
updated_date="2024-04-16"
) }}

