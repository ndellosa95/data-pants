{{ config(
    materialized="view",
    tags=["mnpi"]
) }}



WITH zuora_invoice_aging_detail AS (

/* grain: transaction ID. The Invoice Aging detail report provides a list of invoices that have outstanding amounts as of the end of the accounting period.  */

    SELECT *
    FROM {{ source('zuora', 'invoice_aging_detail') }}
    
), final_invoice_aging_detail AS (

SELECT
   -- primary key 
      zuora_invoice_aging_detail.id                          	               AS invoice_aging_detail_id,

   -- keys
      zuora_invoice_aging_detail.invoiceid               	                   AS invoice_id,     
      zuora_invoice_aging_detail.accountingperiodid                  	       AS accounting_period_id,

   -- invoice aging detail dates
      zuora_invoice_aging_detail.accountingperiodenddate                     AS accounting_period_end_date,
     {{ get_date_id('zuora_invoice_aging_detail.accountingperiodenddate') }} AS accounting_period_end_date_id,

   -- additive fields
      zuora_invoice_aging_detail.accountbalanceimpact                      	 AS account_balance_impact,
      zuora_invoice_aging_detail.daysoverdue                     	           AS days_overdue
    


FROM zuora_invoice_aging_detail
)

{{ dbt_audit(
cte_ref="final_invoice_aging_detail",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-02-28",
updated_date="2024-02-28"
) }}
