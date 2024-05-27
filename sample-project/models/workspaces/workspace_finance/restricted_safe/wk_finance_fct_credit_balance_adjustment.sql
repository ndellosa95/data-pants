{{ config(
    materialized="view",
    tags=["mnpi"]
) }}



WITH zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }} 
    WHERE is_deleted = FALSE

), zuora_credit_balance_adjustment AS (

/* grain: lowest grain is a credit balance adjustment made on the invoice or account */

    SELECT *
    FROM {{ source('zuora', 'credit_balance_adjustment') }}
    
), final_credit_balance_adjustment AS (

SELECT
   -- primary key 
      zuora_credit_balance_adjustment.id                          		AS credit_balance_adjustment_id,

   -- keys
      zuora_credit_balance_adjustment.number                          AS credit_balance_adjustment_number,
      zuora_credit_balance_adjustment.accountid                  		  AS account_id,
      zuora_credit_balance_adjustment.invoiceid                  		  AS invoice_id,
      zuora_credit_balance_adjustment.accountingperiodid              AS accounting_period_id,


   -- credit balance adjustment dates
      zuora_credit_balance_adjustment.adjustmentdate                     AS credit_balance_adjustment_date,
     {{ get_date_id('zuora_credit_balance_adjustment.adjustmentdate') }} AS credit_balance_adjustment_date_id,

   -- additive fields
      zuora_credit_balance_adjustment.amount                        	AS credit_balance_adjustment_amount,
      zuora_credit_balance_adjustment.status                      		AS credit_balance_adjustment_status,
      zuora_credit_balance_adjustment.type                     		    AS credit_balance_adjustment_type




FROM zuora_credit_balance_adjustment
INNER JOIN zuora_account
  ON zuora_credit_balance_adjustment.accountid = zuora_account.account_id
)

{{ dbt_audit(
cte_ref="final_credit_balance_adjustment",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-02-28",
updated_date="2024-03-26"
) }}
