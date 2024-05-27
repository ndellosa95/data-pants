{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

WITH final AS (

/* View refund amounts processed monthly */

SELECT 
DATE_TRUNC('month',(DATE(refund_date))) AS refund_period,
SUM(refund_amount) AS refund_amount,
FROM {{ ref('wk_finance_fct_refund') }} 
WHERE refund_status = 'Processed'
GROUP BY refund_period
ORDER BY refund_period

)


{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-03-26",
updated_date="2024-04-08"
) }}

