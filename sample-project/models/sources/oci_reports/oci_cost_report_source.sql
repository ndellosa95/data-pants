WITH source as (

    SELECT *

      FROM {{ source('oci_reports', 'oci_cost_report') }}

), renamed as (

    SELECT DISTINCT

      "lineItem/referenceNo"                        AS line_item_reference_no,
      "lineItem/tenantId"                           AS line_item_tenant_id,
      "lineItem/intervalUsageStart"                 AS line_item_interval_usage_start,
      "lineItem/intervalUsageEnd"                   AS line_item_interval_usage_end,
      "product/service"                             AS product_service,
      "product/compartmentId"                       AS product_compartment_id,
      "product/compartmentName"                     AS product_compartment_name,
      "product/region"                              AS product_region,
      "product/availabilityDomain"                  AS product_availability_domain,
      "product/resourceId"                          AS product_resource_id,
      "usage/billedQuantity"                        AS usage_billed_quantity,
      "usage/billedQuantityOverage"                 AS usage_billed_quantity_overage,
      "cost/subscriptionId"                         AS cost_subscription_id,
      "cost/productSku"                             AS cost_product_sku,
      "product/Description"                         AS product_description,
      "cost/unitPrice"                              AS cost_unit_price,
      "cost/unitPriceOverage"                       AS cost_unit_price_overage,
      "cost/myCost"                                 AS cost_my_cost,
      "cost/myCostOverage"                          AS cost_my_cost_overage,
      "cost/currencyCode"                           AS cost_currency_code,
      "cost/billingUnitReadable"                    AS cost_billing_unit_readable,
      "cost/skuUnitDescription"                     AS cost_sku_unit_description,
      "cost/overageFlag"                            AS cost_overage_flag,
      "lineItem/isCorrection"                       AS line_item_is_correction,
      "lineItem/backreferenceNo"                    AS line_item_back_reference_no,
      "tags/OKEnodePoolName"                        AS tags_oke_node_pool_name,
      "tags/Oracle-Tags.CreatedBy"                  AS tags_oracle_tags_created_by,
      "tags/Oracle-Tags.CreatedOn"                  AS tags_oracle_tags_created_on,
      "tags/cluster_name"                           AS tags_cluster_name,
      "tags/oci:compute:instanceconfiguration"      AS tags_oci_compute_instance_configuration,
      "tags/oci:compute:instancepool"               AS tags_oci_compute_instance_pool,
      "tags/oci:compute:instancepool:opcretrytoken" AS tags_oci_compute_instance_pool_opc_retry_token,
      "tags/orcl-cloud.free-tier-retained"          AS tags_orcl_cloud_free_tier_retained,
      "tags/parent_cluster"                         AS tags_parent_cluster

    FROM source

)

SELECT *
FROM renamed