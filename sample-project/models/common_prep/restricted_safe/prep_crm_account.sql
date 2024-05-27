{{ sfdc_account_fields('live') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@chrissharp",
    created_date="2020-06-01",
    updated_date="2024-03-25"
) }}
