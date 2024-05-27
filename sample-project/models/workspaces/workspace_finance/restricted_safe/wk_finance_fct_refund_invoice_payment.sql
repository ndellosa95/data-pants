{{ config(
    materialized="view",
    tags=["mnpi"]
) }}



WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }} 
    WHERE is_deleted = FALSE

), zuora_refund_invoice_payment AS (

/* grain: lowest grain a refund made on the invoice */

    SELECT *
    FROM {{ source('zuora', 'refund_invoice_payment') }}
    
), final_refund_invoice_payment AS (

SELECT
   -- primary key 
      zuora_refund_invoice_payment.id                          		AS refund_invoice_payment_id,

   -- keys
      zuora_refund_invoice_payment.invoiceid		                  AS invoice_id,
     zuora_refund_invoice_payment.refundid               		      AS refund_id,  
      zuora_refund_invoice_payment.paymentid               		    AS payment_id,     
      zuora_refund_invoice_payment.accountid                  		AS account_id,
      zuora_refund_invoice_payment.accountingperiodid             AS accounting_period_id,


   -- refund invoice payment dates
      zuora_refund_invoice_payment.createddate                     AS refund_invoice_payment_date,
     {{ get_date_id('zuora_refund_invoice_payment.createddate') }} AS refund_invoice_payment_date_id,

   -- additive fields
      zuora_refund_invoice_payment.refundamount                    AS refund_invoice_payment_amount



FROM zuora_refund_invoice_payment
INNER JOIN zuora_account
  ON zuora_refund_invoice_payment.accountid = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final_refund_invoice_payment",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-02-28",
updated_date="2024-02-28"
) }}
