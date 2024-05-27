{{ config({
        "materialized": "table",
        "transient": false,
        "alias": "mart_arr_all"
    })
}}

{{ simple_cte([
    ('dim_billing_account','dim_billing_account'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_product_detail','dim_product_detail'),
    ('dim_subscription','dim_subscription'),
    ('dim_date', 'dim_date'),
    ('fct_mrr_with_zero_dollar_charges', 'fct_mrr_with_zero_dollar_charges')
]) }}

, fct_mrr_all AS (

    SELECT
      dim_date_id,
      dim_subscription_id,
      dim_product_detail_id,
      dim_billing_account_id,
      dim_crm_account_id,
      SUM(mrr)                                                                      AS mrr,
      SUM(arr)                                                                      AS arr,
      SUM(quantity)                                                                 AS quantity,
      ARRAY_AGG(DISTINCT unit_of_measure) WITHIN GROUP (ORDER BY unit_of_measure)   AS unit_of_measure

    FROM {{ ref('fct_mrr_with_zero_dollar_charges') }}
    WHERE subscription_status IN ('Active', 'Cancelled')
    {{ dbt_utils.group_by(n=5) }}

), joined AS (

    SELECT
      --primary_key
      {{ dbt_utils.generate_surrogate_key(['fct_mrr_all.dim_date_id', 'dim_subscription.subscription_name', 'fct_mrr_all.dim_product_detail_id']) }}
                                                                                      AS primary_key,

      --date info
      dim_date.date_actual                                                            AS arr_month,
      IFF(is_first_day_of_last_month_of_fiscal_quarter, fiscal_quarter_name_fy, NULL) AS fiscal_quarter_name_fy,
      IFF(is_first_day_of_last_month_of_fiscal_year, fiscal_year, NULL)               AS fiscal_year,
      dim_subscription.subscription_start_month                                       AS subscription_start_month,
      dim_subscription.subscription_end_month                                         AS subscription_end_month,

      --billing account info
      dim_billing_account.dim_billing_account_id                                      AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                             AS sold_to_country,
      dim_billing_account.billing_account_name                                        AS billing_account_name,
      dim_billing_account.billing_account_number                                      AS billing_account_number,
      dim_billing_account.ssp_channel                                                 AS ssp_channel,
      dim_billing_account.po_required                                                 AS po_required,
      dim_billing_account.auto_pay                                                    AS auto_pay,
      dim_billing_account.default_payment_method_type                                 AS default_payment_method_type,

      -- crm account info
      dim_crm_account.dim_crm_account_id                                              AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                       AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name                                         AS parent_crm_account_name,
      dim_crm_account.parent_crm_account_sales_segment                                AS parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_industry                                     AS parent_crm_account_industry,
      dim_crm_account.crm_account_employee_count_band                                 AS crm_account_employee_count_band,
      dim_crm_account.health_score_color                                              AS health_score_color,
      dim_crm_account.health_number                                                   AS health_number,
      dim_crm_account.is_jihu_account                                                 AS is_jihu_account,
      dim_crm_account.parent_crm_account_lam                                          AS parent_crm_account_lam,
      dim_crm_account.parent_crm_account_lam_dev_count                                AS parent_crm_account_lam_dev_count,
      dim_crm_account.parent_crm_account_business_unit                                AS parent_crm_account_business_unit,
      dim_crm_account.parent_crm_account_geo                                          AS parent_crm_account_geo,
      dim_crm_account.parent_crm_account_region                                       AS parent_crm_account_region,
      dim_crm_account.parent_crm_account_area                                         AS parent_crm_account_area,
      dim_crm_account.parent_crm_account_role_type                                    AS parent_crm_account_role_type,
      dim_crm_account.parent_crm_account_territory                                    AS parent_crm_account_territory,
      dim_crm_account.parent_crm_account_max_family_employee                          AS parent_crm_account_max_family_employee,
      dim_crm_account.parent_crm_account_upa_country                                  AS parent_crm_account_upa_country,
      dim_crm_account.parent_crm_account_upa_state                                    AS parent_crm_account_upa_state,
      dim_crm_account.parent_crm_account_upa_city                                     AS parent_crm_account_upa_city,
      dim_crm_account.parent_crm_account_upa_street                                   AS parent_crm_account_upa_street,
      dim_crm_account.parent_crm_account_upa_postal_code                              AS parent_crm_account_upa_postal_code,
      dim_crm_account.crm_account_employee_count                                      AS crm_account_employee_count,

      --subscription info
      dim_subscription.dim_subscription_id                                            AS dim_subscription_id,
      dim_subscription.dim_subscription_id_original                                   AS dim_subscription_id_original,
      dim_subscription.subscription_status                                            AS subscription_status,
      dim_subscription.subscription_sales_type                                        AS subscription_sales_type,
      dim_subscription.subscription_name                                              AS subscription_name,
      dim_subscription.subscription_name_slugify                                      AS subscription_name_slugify,
      dim_subscription.oldest_subscription_in_cohort                                  AS oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage                                           AS subscription_lineage,
      dim_subscription.subscription_cohort_month                                      AS subscription_cohort_month,
      dim_subscription.subscription_cohort_quarter                                    AS subscription_cohort_quarter,
      dim_billing_account.billing_account_arr_cohort_month                            AS billing_account_cohort_month,
      dim_billing_account.billing_account_arr_cohort_quarter                          AS billing_account_cohort_quarter,
      dim_crm_account.crm_account_arr_cohort_month                                    AS crm_account_cohort_month,
      dim_crm_account.crm_account_arr_cohort_quarter                                  AS crm_account_cohort_quarter,
      dim_crm_account.parent_account_arr_cohort_month                                 AS parent_account_cohort_month,
      dim_crm_account.parent_account_arr_cohort_quarter                               AS parent_account_cohort_quarter,
      dim_subscription.auto_renew_native_hist,
      dim_subscription.auto_renew_customerdot_hist,
      dim_subscription.turn_on_cloud_licensing,
      dim_subscription.turn_on_operational_metrics,
      dim_subscription.contract_operational_metrics,
      dim_subscription.contract_auto_renewal,
      dim_subscription.turn_on_auto_renewal,
      dim_subscription.contract_seat_reconciliation,
      dim_subscription.turn_on_seat_reconciliation,
      dim_subscription.invoice_owner_account,
      dim_subscription.creator_account,
      dim_subscription.was_purchased_through_reseller,

      --product info
      dim_product_detail.dim_product_detail_id                                        AS dim_product_detail_id,
      dim_product_detail.product_tier_name                                            AS product_tier_name,
      dim_product_detail.product_delivery_type                                        AS product_delivery_type,
      dim_product_detail.product_deployment_type                                      AS product_deployment_type,
      dim_product_detail.product_category                                             AS product_category,
      dim_product_detail.product_ranking                                              AS product_ranking,
      dim_product_detail.service_type                                                 AS service_type,
      dim_product_detail.product_rate_plan_name                                       AS product_rate_plan_name,
      dim_product_detail.is_licensed_user                                             AS is_licensed_user,
      dim_product_detail.is_arpu                                                      AS is_arpu,

      -- MRR values
      --  not needed as all charges in fct_mrr are recurring
      --  fct_mrr.charge_type,
      fct_mrr_all.unit_of_measure                                                         AS unit_of_measure,
      fct_mrr_all.mrr                                                                     AS mrr,
      fct_mrr_all.arr                                                                     AS arr,
      fct_mrr_all.quantity                                                                AS quantity
    FROM fct_mrr_all
    INNER JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr_all.dim_subscription_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr_all.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_mrr_all.dim_billing_account_id
    INNER JOIN dim_date
      ON dim_date.date_id = fct_mrr_all.dim_date_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    WHERE dim_crm_account.is_jihu_account != 'TRUE'

), cohort_diffs AS (

  SELECT
    --primary_key
    primary_key,

    --date info
    arr_month,
    fiscal_quarter_name_fy,
    fiscal_year,
    subscription_start_month,
    subscription_end_month,

    --billing account info
    dim_billing_account_id,
    sold_to_country,
    billing_account_name,
    billing_account_number,
    ssp_channel,
    po_required,
    auto_pay,
    default_payment_method_type,

    -- crm account info
    dim_crm_account_id,
    crm_account_name,
    dim_parent_crm_account_id,
    parent_crm_account_name,
    parent_crm_account_sales_segment,
    parent_crm_account_industry,
    crm_account_employee_count_band,
    health_score_color,
    health_number,
    is_jihu_account,
    parent_crm_account_lam,
    parent_crm_account_lam_dev_count,
    parent_crm_account_business_unit,
    parent_crm_account_geo,
    parent_crm_account_region,
    parent_crm_account_area,
    parent_crm_account_role_type,
    parent_crm_account_territory,
    parent_crm_account_max_family_employee,
    parent_crm_account_upa_country,
    parent_crm_account_upa_state,
    parent_crm_account_upa_city,
    parent_crm_account_upa_street,
    parent_crm_account_upa_postal_code,
    crm_account_employee_count,

    --subscription info
    dim_subscription_id,
    dim_subscription_id_original,
    subscription_status,
    subscription_sales_type,
    subscription_name,
    subscription_name_slugify,
    oldest_subscription_in_cohort,
    subscription_lineage,
    subscription_cohort_month,
    subscription_cohort_quarter,
    billing_account_cohort_month,
    billing_account_cohort_quarter,
    crm_account_cohort_month,
    crm_account_cohort_quarter,
    parent_account_cohort_month,
    parent_account_cohort_quarter,
    auto_renew_native_hist,
    auto_renew_customerdot_hist,
    turn_on_cloud_licensing,
    turn_on_operational_metrics,
    contract_operational_metrics,
    contract_auto_renewal,
    turn_on_auto_renewal,
    contract_seat_reconciliation,
    turn_on_seat_reconciliation,
    invoice_owner_account,
    creator_account,
    was_purchased_through_reseller,

    --product info
    dim_product_detail_id,
    product_tier_name,
    product_delivery_type,
    product_deployment_type,
    product_category,
    product_ranking,
    service_type,
    product_rate_plan_name,
    is_licensed_user,
    is_arpu,
  
    -- MRR values
    --  not needed as all charges in fct_mrr are recurring
    --  fct_mrr.charge_type,
    unit_of_measure,
    mrr,
    arr,
    quantity,
    datediff(month, billing_account_cohort_month, arr_month)     AS months_since_billing_account_cohort_start,
    datediff(quarter, billing_account_cohort_quarter, arr_month) AS quarters_since_billing_account_cohort_start,
    datediff(month, crm_account_cohort_month, arr_month)         AS months_since_crm_account_cohort_start,
    datediff(quarter, crm_account_cohort_quarter, arr_month)     AS quarters_since_crm_account_cohort_start,
    datediff(month, parent_account_cohort_month, arr_month)      AS months_since_parent_account_cohort_start,
    datediff(quarter, parent_account_cohort_quarter, arr_month)  AS quarters_since_parent_account_cohort_start,
    datediff(month, subscription_cohort_month, arr_month)        AS months_since_subscription_cohort_start,
    datediff(quarter, subscription_cohort_quarter, arr_month)    AS quarters_since_subscription_cohort_start
  FROM joined

), parent_arr AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      SUM(arr)                                   AS arr
    FROM joined
    {{ dbt_utils.group_by(n=2) }}

), parent_arr_band_calc AS (

    SELECT
      arr_month,
      dim_parent_crm_account_id,
      CASE
        WHEN arr > 5000 THEN 'ARR > $5K'
        WHEN arr <= 5000 THEN 'ARR <= $5K'
      END                                        AS arr_band_calc
    FROM parent_arr

), final_table AS (

    SELECT
      --primary_key
      cohort_diffs.primary_key,

      --Foreign Keys
      cohort_diffs.arr_month,
      cohort_diffs.dim_billing_account_id,
      cohort_diffs.dim_crm_account_id,
      cohort_diffs.dim_subscription_id,
      cohort_diffs.dim_product_detail_id,

      --date info
      cohort_diffs.fiscal_quarter_name_fy,
      cohort_diffs.fiscal_year,
      cohort_diffs.subscription_start_month,
      cohort_diffs.subscription_end_month,

      --billing account info
      cohort_diffs.sold_to_country,
      cohort_diffs.billing_account_name,
      cohort_diffs.billing_account_number,
      cohort_diffs.ssp_channel,
      cohort_diffs.po_required,
      cohort_diffs.auto_pay,
      cohort_diffs.default_payment_method_type,

      -- crm account info
      cohort_diffs.crm_account_name,
      cohort_diffs.dim_parent_crm_account_id,
      cohort_diffs.parent_crm_account_name,
      cohort_diffs.parent_crm_account_sales_segment,
      cohort_diffs.parent_crm_account_industry,
      cohort_diffs.crm_account_employee_count_band,
      cohort_diffs.health_score_color,
      cohort_diffs.health_number,
      cohort_diffs.is_jihu_account,
      cohort_diffs.parent_crm_account_lam,
      cohort_diffs.parent_crm_account_lam_dev_count,
      cohort_diffs.parent_crm_account_business_unit,
      cohort_diffs.parent_crm_account_geo,
      cohort_diffs.parent_crm_account_region,
      cohort_diffs.parent_crm_account_area,
      cohort_diffs.parent_crm_account_role_type,
      cohort_diffs.parent_crm_account_territory,
      cohort_diffs.parent_crm_account_max_family_employee,
      cohort_diffs.parent_crm_account_upa_country,
      cohort_diffs.parent_crm_account_upa_state,
      cohort_diffs.parent_crm_account_upa_city,
      cohort_diffs.parent_crm_account_upa_street,
      cohort_diffs.parent_crm_account_upa_postal_code,
      cohort_diffs.crm_account_employee_count,

      --subscription info
      cohort_diffs.dim_subscription_id_original,
      cohort_diffs.subscription_status,
      cohort_diffs.subscription_sales_type,
      cohort_diffs.subscription_name,
      cohort_diffs.subscription_name_slugify,
      cohort_diffs.oldest_subscription_in_cohort,
      cohort_diffs.subscription_lineage,
      cohort_diffs.subscription_cohort_month,
      cohort_diffs.subscription_cohort_quarter,
      cohort_diffs.billing_account_cohort_month,
      cohort_diffs.billing_account_cohort_quarter,
      cohort_diffs.crm_account_cohort_month,
      cohort_diffs.crm_account_cohort_quarter,
      cohort_diffs.parent_account_cohort_month,
      cohort_diffs.parent_account_cohort_quarter,
      cohort_diffs.auto_renew_native_hist,
      cohort_diffs.auto_renew_customerdot_hist,
      cohort_diffs.turn_on_cloud_licensing,
      cohort_diffs.turn_on_operational_metrics,
      cohort_diffs.contract_operational_metrics,
      cohort_diffs.contract_auto_renewal,
      cohort_diffs.turn_on_auto_renewal,
      cohort_diffs.contract_seat_reconciliation,
      cohort_diffs.turn_on_seat_reconciliation,
      cohort_diffs.invoice_owner_account,
      cohort_diffs.creator_account,
      cohort_diffs.was_purchased_through_reseller,

      --product info
      cohort_diffs.product_tier_name,
      cohort_diffs.product_delivery_type,
      cohort_diffs.product_deployment_type,
      cohort_diffs.product_category,
      cohort_diffs.product_ranking,
      cohort_diffs.service_type,
      cohort_diffs.product_rate_plan_name,
      cohort_diffs.is_licensed_user,
      cohort_diffs.is_arpu,
    
      -- MRR values
      cohort_diffs.unit_of_measure,
      cohort_diffs.mrr,
      cohort_diffs.arr,
      cohort_diffs.quantity,
      cohort_diffs.months_since_billing_account_cohort_start,
      cohort_diffs.quarters_since_billing_account_cohort_start,
      cohort_diffs.months_since_crm_account_cohort_start,
      cohort_diffs.quarters_since_crm_account_cohort_start,
      cohort_diffs.months_since_parent_account_cohort_start,
      cohort_diffs.quarters_since_parent_account_cohort_start,
      cohort_diffs.months_since_subscription_cohort_start,
      cohort_diffs.quarters_since_subscription_cohort_start,
      parent_arr_band_calc.arr_band_calc

    FROM cohort_diffs
    LEFT JOIN parent_arr_band_calc
      ON cohort_diffs.arr_month = parent_arr_band_calc.arr_month
      AND cohort_diffs.dim_parent_crm_account_id = parent_arr_band_calc.dim_parent_crm_account_id

)

{{ dbt_audit(
    cte_ref="final_table",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2023-12-01",
    updated_date="2024-04-10"
) }}
