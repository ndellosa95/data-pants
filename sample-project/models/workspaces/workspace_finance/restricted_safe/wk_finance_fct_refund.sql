{{ config(
    materialized="view",
    tags=["mnpi"]
) }}



WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }} 
    WHERE is_deleted = FALSE

), zuora_refund AS (

/* grain: lowest grain is a refund made it off an invoice or account */

    SELECT *
    FROM {{ source('zuora', 'refund') }}
    
), final_refund AS (

SELECT
   -- primary key 
      zuora_refund.id                          		AS refund_id,

   -- keys
      zuora_refund.accountid                  		AS account_id,
      zuora_refund.refundnumber                  	AS refund_number,


   -- refund dates
      zuora_refund.refunddate                     AS refund_date,
     {{ get_date_id('zuora_refund.refunddate') }} AS refund_date_id,

   -- additive fields
      zuora_refund.amount                        	AS refund_amount,
      zuora_refund.status                      		AS refund_status,
      zuora_refund.type                     		  AS refund_type




FROM zuora_refund
INNER JOIN zuora_account
  ON zuora_refund.accountid = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final_refund",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-02-28",
updated_date="2024-02-28"
) }}
