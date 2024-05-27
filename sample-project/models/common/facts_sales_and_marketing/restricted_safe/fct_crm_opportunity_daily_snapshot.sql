{{ config(
    materialized="incremental",
    unique_key="crm_opportunity_snapshot_id"
) }}

WITH final AS (

  SELECT {{ dbt_utils.star(from=ref('prep_crm_opportunity'), except=["CREATED_BY", "UPDATED_BY", "MODEL_CREATED_DATE", "MODEL_UPDATED_DATE", "DBT_UPDATED_AT", "DBT_CREATED_AT"]) }}
  FROM {{ ref('prep_crm_opportunity') }}
  WHERE is_live = 0
  {% if is_incremental() %}
  
    AND snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})

  {% endif %}


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@snalamaru",
    created_date="2022-02-23",
    updated_date="2024-02-21"
) }}