{{ config(
    tags=["six_hourly"]
) }}

{{ sfdc_user_fields('live') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@chrissharp",
    created_date="2021-01-12",
    updated_date="2024-02-28"
) }}
