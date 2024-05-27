{{ config(
    materialized="table",
    tags=["mnpi"]
) }}

/* grain: transaction ID. The Invoice Aging detail report provides a list of invoices that have outstanding amounts as of the end of the accounting period.  */

{{ simple_cte([
    ('zuora_invoice_aging_detail_source', 'zuora_invoice_aging_detail_source')
]) }},

final AS (

  SELECT
    --Primary key 
    {{ dbt_utils.generate_surrogate_key(['zuora_invoice_aging_detail_source.invoice_aging_detail_id']) }} AS invoice_aging_detail_pk,

    --Natural key 
    zuora_invoice_aging_detail_source.invoice_aging_detail_id,

    --Foreign keys
    zuora_invoice_aging_detail_source.invoice_id AS dim_invoice_id,
    zuora_invoice_aging_detail_source.accounting_period_id,

    --Invoice aging detail dates
    zuora_invoice_aging_detail_source.accounting_period_end_date,
    {{ get_date_id('zuora_invoice_aging_detail_source.accounting_period_end_date') }} AS accounting_period_end_date_id,

    --Additive fields
    zuora_invoice_aging_detail_source.account_balance_impact,
    zuora_invoice_aging_detail_source.days_overdue



  FROM zuora_invoice_aging_detail_source
)

{{ dbt_audit(
cte_ref="final",
created_by="@apiaseczna",
updated_by="@apiaseczna",
created_date="2024-05-01",
updated_date="2024-05-01"
) }}

