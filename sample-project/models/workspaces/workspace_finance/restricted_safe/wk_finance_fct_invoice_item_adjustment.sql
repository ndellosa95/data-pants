{{ config(
    materialized="view",
    tags=["mnpi"]
) }}



WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }} 
    WHERE is_deleted = FALSE

), zuora_invoice_item_adjustment AS (

/* grain: lowest grain is a invoice item adjustment made on the invoice */

    SELECT *
    FROM {{ source('zuora', 'invoice_item_adjustment') }}
    
), final_invoice_item_adjustment AS (

SELECT
   -- primary key 
      zuora_invoice_item_adjustment.id                          		AS invoice_item_adjustment_id,

   -- keys
      zuora_invoice_item_adjustment.adjustmentnumber                AS invoice_item_adjustment_number,
      zuora_invoice_item_adjustment.accountid                  		  AS account_id,
      zuora_invoice_item_adjustment.invoiceid                  		  AS invoice_id,
      zuora_invoice_item_adjustment.accountingperiodid              AS accounting_period_id,


   -- invoice item adjustment dates
      zuora_invoice_item_adjustment.adjustmentdate                  AS invoice_item_adjustment_date,
     {{ get_date_id('zuora_invoice_item_adjustment.adjustmentdate') }} AS invoice_item_adjustment_date_id,

   -- additive fields
      zuora_invoice_item_adjustment.amount                        	AS invoice_item_adjustment_amount,
      zuora_invoice_item_adjustment.status                      		AS invoice_item_adjustment_status,
      zuora_invoice_item_adjustment.type                     		    AS invoice_item_adjustment_type




FROM zuora_invoice_item_adjustment
INNER JOIN zuora_account
  ON zuora_invoice_item_adjustment.accountid = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final_invoice_item_adjustment",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-02-28",
updated_date="2024-03-26"
) }}
