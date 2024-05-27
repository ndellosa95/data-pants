-- depends_on: {{ ref('zuora_excluded_accounts') }}

{{ config(
    materialized="view",
    tags=["mnpi"]
) }}

WITH source AS (

  SELECT *
  FROM {{ source('zuora', 'invoice_aging_detail') }}

),

renamed AS (

  SELECT
    -- primary key 
    id                      AS invoice_aging_detail_id,

    -- keys
    invoiceid               AS invoice_id,
    accountingperiodid      AS accounting_period_id,

    -- invoice aging detail dates
    accountingperiodenddate AS accounting_period_end_date,

    -- additive fields
    accountbalanceimpact    AS account_balance_impact,
    daysoverdue             AS days_overdue


  FROM source

)

SELECT *
FROM renamed
