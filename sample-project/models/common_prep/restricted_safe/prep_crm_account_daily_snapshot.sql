{{ config({
        "materialized": "incremental",
        "unique_key": "crm_account_snapshot_id",
        "tags": ["account_snapshots"],
    })
}}

{{sfdc_account_fields('snapshot') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2023-03-27",
    updated_date="2024-03-25"
) }}
