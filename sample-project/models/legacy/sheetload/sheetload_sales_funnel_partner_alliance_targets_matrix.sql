WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sales_funnel_partner_alliance_targets_matrix_source') }}

), final AS (

    SELECT
      kpi_name,
      month,
      sales_qualified_source,
      alliance_partner,
      partner_category,
      order_type,
      area,
      user_segment,
      user_geo,
      user_region,
      user_area,
      user_business_unit,
      allocated_target
    FROM source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-04-05",
    updated_date="2023-03-10"
) }}
