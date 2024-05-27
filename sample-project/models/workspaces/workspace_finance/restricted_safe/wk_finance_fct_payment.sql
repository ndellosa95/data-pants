{{ config(
    materialized="view",
    tags=["mnpi"]
) }}



WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }} 
    WHERE is_deleted = FALSE

), zuora_payment AS (

/* grain: lowest grain one payment, one payment may be split and applied to several invoices */

    SELECT *
    FROM {{ source('zuora', 'payment') }}
    
), final_payment AS (

SELECT
   -- primary key 
      zuora_payment.id                          		AS payment_id,

   -- keys
      zuora_payment.paymentnumber               		AS payment_number,     
      zuora_payment.accountid                  			AS account_id,

   -- payment dates
      zuora_payment.effectivedate                     AS payment_date,
     {{ get_date_id('zuora_payment.effectivedate') }} AS payment_date_id,

   -- additive fields
      zuora_payment.status                      		AS payment_status,
      zuora_payment.type                     			AS payment_type,
      zuora_payment.amount                        		AS payment_amount



FROM zuora_payment
INNER JOIN zuora_account
  ON zuora_payment.accountid = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final_payment",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-02-26",
updated_date="2024-02-26"
) }}
