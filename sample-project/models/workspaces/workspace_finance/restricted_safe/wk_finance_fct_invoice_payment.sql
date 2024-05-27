{{ config(
    materialized="view",
    tags=["mnpi"]
) }}



WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }} 
    WHERE is_deleted = FALSE

), zuora_invoice_payment AS (

/* grain: lowest grain a payment applied to the invoice */

    SELECT *
    FROM {{ source('zuora', 'invoice_payment') }}
    
), final_invoice_payment AS (

SELECT
   -- primary key 
      zuora_invoice_payment.id                          		AS invoice_payment_id,

   -- keys
      zuora_invoice_payment.invoiceid		                    AS invoice_id,
      zuora_invoice_payment.paymentid               		    AS payment_id,     
      zuora_invoice_payment.accountid                  		  AS account_id,
      zuora_invoice_payment.accountingperiodid              AS accounting_period_id,


   -- invoice payment dates
      zuora_invoice_payment.createddate                     AS invoice_payment_date,
     {{ get_date_id('zuora_invoice_payment.createddate') }} AS invoice_payment_date_id,

   -- additive fields
      zuora_invoice_payment.amount                        	AS invoice_payment_amount



FROM zuora_invoice_payment
INNER JOIN zuora_account
  ON zuora_invoice_payment.accountid = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final_invoice_payment",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-02-28",
updated_date="2024-02-28"
) }}
