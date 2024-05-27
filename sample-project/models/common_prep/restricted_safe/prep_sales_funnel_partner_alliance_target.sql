{{ simple_cte([
      ('dim_date', 'dim_date'),
      ('sheetload_sales_funnel_partner_alliance_targets_matrix_source', 'sheetload_sales_funnel_partner_alliance_targets_matrix_source')
])}}

, fiscal_months AS (

    SELECT DISTINCT
      fiscal_month_name_fy,
      fiscal_year,
      first_day_of_month
    FROM dim_date

), final AS (

  SELECT
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.kpi_name,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.month,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.sales_qualified_source,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.order_type,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.area,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.allocated_target,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_business_unit,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.alliance_partner,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.partner_category,
    sheetload_sales_funnel_partner_alliance_targets_matrix_source.sqs_bucket_engagement,
    CASE
      WHEN fiscal_months.fiscal_year < 2024
        THEN CONCAT(
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment),
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo), 
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region), 
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area),
                    '-',
                    fiscal_months.fiscal_year
                    )
      WHEN fiscal_months.fiscal_year >= 2024 AND LOWER(user_business_unit) = 'comm'
        THEN CONCAT(
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_business_unit),
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo), 
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment), 
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region), 
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area),
                    '-',
                    fiscal_months.fiscal_year
                    )
      WHEN fiscal_months.fiscal_year >= 2024 AND LOWER(user_business_unit) = 'entg'
        THEN CONCAT(
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_business_unit),
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo), 
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region), 
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area), 
                    '-',
                    UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment),
                    '-',
                    fiscal_months.fiscal_year
                    )
      WHEN fiscal_months.fiscal_year >= 2024 
        AND (sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_business_unit IS NOT NULL AND LOWER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_business_unit) NOT IN ('comm', 'entg')) -- account for non-sales reps
          THEN CONCAT(
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_business_unit), 
                      '-',
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment), 
                      '-',
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo), 
                      '-',
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region), 
                      '-',
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area),
                      '-',
                      fiscal_months.fiscal_year
                      )

        WHEN fiscal_months.fiscal_year >= 2024 AND sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_business_unit IS NULL -- account for nulls/possible data issues
          THEN CONCAT(
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_segment), 
                      '-',
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_geo), 
                      '-',
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_region), 
                      '-',
                      UPPER(sheetload_sales_funnel_partner_alliance_targets_matrix_source.user_area),
                      '-',
                      fiscal_months.fiscal_year
                      )
      END                                                                                                                           AS dim_crm_user_hierarchy_sk,
      fiscal_months.fiscal_year,
      fiscal_months.first_day_of_month
  FROM sheetload_sales_funnel_partner_alliance_targets_matrix_source
  INNER JOIN fiscal_months
    ON {{ sales_funnel_text_slugify("sheetload_sales_funnel_partner_alliance_targets_matrix_source.month") }} = {{ sales_funnel_text_slugify("fiscal_months.fiscal_month_name_fy") }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-03-10",
    updated_date="2023-04-24"
) }}
