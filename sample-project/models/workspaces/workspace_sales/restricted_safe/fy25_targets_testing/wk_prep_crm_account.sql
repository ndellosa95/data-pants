{{ sfdc_sandbox_account_fields('live') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2024-03-20",
    updated_date="2024-04-08"
) }}
