{{ config({
        "materialized": "incremental",
        "unique_key": "crm_user_snapshot_id",
        "tags": ["user_snapshots"],
    })
}}

{{sfdc_user_fields('snapshot') }}

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@chrissharp",
    created_date="2023-03-10",
    updated_date="2024-03-28"
) }}
