{{ config(
    materialized='incremental',
    )
}}

WITH dedicated_legacy_0475 AS (

  SELECT
    *,
    metadata$file_last_modified AS modified_at_
  FROM {{ source('aws_billing', 'dedicated_legacy_0475') }}
  {% if is_incremental() %}

    WHERE metadata$file_last_modified >= (SELECT MAX(modified_at) FROM {{ this }})

  {% endif %}

),

dedicated_dev_3675 AS (

  SELECT
    *,
    metadata$file_last_modified AS modified_at_
  FROM {{ source('aws_billing', 'dedicated_dev_3675') }}
  {% if is_incremental() %}

    WHERE metadata$file_last_modified >= (SELECT MAX(modified_at) FROM {{ this }})

  {% endif %}

),

gitlab_marketplace_5127 AS (

  SELECT
    *,
    metadata$file_last_modified AS modified_at_
  FROM {{ source('aws_billing', 'gitlab_marketplace_5127') }}
  {% if is_incremental() %}

    WHERE metadata$file_last_modified >= (SELECT MAX(modified_at) FROM {{ this }})

  {% endif %}
),

itorg_3027 AS (

  SELECT
    *,
    metadata$file_last_modified AS modified_at_
  FROM {{ source('aws_billing', 'itorg_3027') }}
  {% if is_incremental() %}

    WHERE metadata$file_last_modified >= (SELECT MAX(modified_at) FROM {{ this }})

  {% endif %}

),

legacy_gitlab_0347 AS (

  SELECT
    *,
    metadata$file_last_modified AS modified_at_
  FROM {{ source('aws_billing', 'legacy_gitlab_0347') }}
  {% if is_incremental() %}

    WHERE metadata$file_last_modified >= (SELECT MAX(modified_at) FROM {{ this }})

  {% endif %}

),

services_org_6953 AS (

  SELECT
    *,
    metadata$file_last_modified AS modified_at_
  FROM {{ source('aws_billing', 'services_org_6953') }}
  {% if is_incremental() %}

    WHERE metadata$file_last_modified >= (SELECT MAX(modified_at) FROM {{ this }})

  {% endif %}

),

all_raw AS (

  SELECT * FROM dedicated_legacy_0475
  UNION ALL
  SELECT * FROM dedicated_dev_3675
  UNION ALL
  SELECT * FROM gitlab_marketplace_5127
  UNION ALL
  SELECT * FROM itorg_3027
  UNION ALL
  SELECT * FROM legacy_gitlab_0347
  UNION ALL
  SELECT * FROM services_org_6953

),

parsed AS (

  SELECT
    value['bill_bill_type']::VARCHAR                                                   AS bill_bill_type,
    value['bill_billing_entity']::VARCHAR                                              AS bill_billing_entity,
    value['bill_billing_period_end_date']::TIMESTAMP                                   AS bill_billing_period_end_date,
    value['bill_billing_period_start_date']::TIMESTAMP                                 AS bill_billing_period_start_date,
    value['bill_invoice_id']::VARCHAR                                                  AS bill_invoice_id,
    value['bill_invoicing_entity']::VARCHAR                                            AS bill_invoicing_entity,
    value['bill_payer_account_id']::VARCHAR                                            AS bill_payer_account_id,
    value['discount_edp_discount']::FLOAT                                              AS discount_edp_discount,
    value['discount_total_discount']::FLOAT                                            AS discount_total_discount,
    value['identity_line_item_id']::VARCHAR                                            AS identity_line_item_id,
    value['identity_time_interval']::VARCHAR                                           AS identity_time_interval,
    value['line_item_availability_zone']::VARCHAR                                      AS line_item_availability_zone,
    value['line_item_blended_cost']::FLOAT                                             AS line_item_blended_cost,
    value['line_item_blended_rate']::VARCHAR                                           AS line_item_blended_rate,
    value['line_item_currency_code']::VARCHAR                                          AS line_item_currency_code,
    value['line_item_legal_entity']::VARCHAR                                           AS line_item_legal_entity,
    value['line_item_line_item_description']::VARCHAR                                  AS line_item_line_item_description,
    value['line_item_line_item_type']::VARCHAR                                         AS line_item_line_item_type,
    value['line_item_net_unblended_cost']::FLOAT                                       AS line_item_net_unblended_cost,
    value['line_item_net_unblended_rate']::VARCHAR                                     AS line_item_net_unblended_rate,
    value['line_item_normalization_factor']::FLOAT                                     AS line_item_normalization_factor,
    value['line_item_normalized_usage_amount']::FLOAT                                  AS line_item_normalized_usage_amount,
    value['line_item_operation']::VARCHAR                                              AS line_item_operation,
    value['line_item_product_code']::VARCHAR                                           AS line_item_product_code,
    value['line_item_resource_id']::VARCHAR                                            AS line_item_resource_id,
    value['line_item_tax_type']::VARCHAR                                               AS line_item_tax_type,
    value['line_item_unblended_cost']::FLOAT                                           AS line_item_unblended_cost,
    value['line_item_unblended_rate']::VARCHAR                                         AS line_item_unblended_rate,
    value['line_item_usage_account_id']::VARCHAR                                       AS line_item_usage_account_id,
    value['line_item_usage_amount']::FLOAT                                             AS line_item_usage_amount,
    TRY_CAST(TO_VARCHAR(value['line_item_usage_start_date']) AS TIMESTAMP_NTZ)         AS line_item_usage_start_date,
    TRY_CAST(TO_VARCHAR(value['line_item_usage_end_date']) AS TIMESTAMP_NTZ)           AS line_item_usage_end_date,
    value['line_item_usage_type']::VARCHAR                                             AS line_item_usage_type,
    value['pricing_currency']::VARCHAR                                                 AS pricing_currency,
    value['pricing_public_on_demand_cost']::FLOAT                                      AS pricing_public_on_demand_cost,
    value['pricing_public_on_demand_rate']::VARCHAR                                    AS pricing_public_on_demand_rate,
    value['pricing_rate_code']::VARCHAR                                                AS pricing_rate_code,
    value['pricing_rate_id']::VARCHAR                                                  AS pricing_rate_id,
    value['pricing_term']::VARCHAR                                                     AS pricing_term,
    value['pricing_unit']::VARCHAR                                                     AS pricing_unit,
    value['product_alarm_type']::VARCHAR                                               AS product_alarm_type,
    value['product_availability']::VARCHAR                                             AS product_availability,
    value['product_availability_zone']::VARCHAR                                        AS product_availability_zone,
    value['product_cache_engine']::VARCHAR                                             AS product_cache_engine,
    value['product_capacitystatus']::VARCHAR                                           AS product_capacitystatus,
    value['product_classicnetworkingsupport']::VARCHAR                                 AS product_classicnetworkingsupport,
    value['product_clock_speed']::VARCHAR                                              AS product_clock_speed,
    value['product_content_type']::VARCHAR                                             AS product_content_type,
    value['product_current_generation']::VARCHAR                                       AS product_current_generation,
    value['product_database_engine']::VARCHAR                                          AS product_database_engine,
    value['product_dedicated_ebs_throughput']::VARCHAR                                 AS product_dedicated_ebs_throughput,
    value['product_deployment_option']::VARCHAR                                        AS product_deployment_option,
    value['product_description']::VARCHAR                                              AS product_description,
    value['product_durability']::VARCHAR                                               AS product_durability,
    value['product_ecu']::VARCHAR                                                      AS product_ecu,
    value['product_engine_code']::VARCHAR                                              AS product_engine_code,
    value['product_enhanced_networking_supported']::VARCHAR                            AS product_enhanced_networking_supported,
    value['product_free_tier']::VARCHAR                                                AS product_free_tier,
    value['product_from_location']::VARCHAR                                            AS product_from_location,
    value['product_from_location_type']::VARCHAR                                       AS product_from_location_type,
    value['product_from_region_code']::VARCHAR                                         AS product_from_region_code,
    value['product_group']::VARCHAR                                                    AS product_group,
    value['product_group_description']::VARCHAR                                        AS product_group_description,
    value['product_instance_family']::VARCHAR                                          AS product_instance_family,
    value['product_instance_type']::VARCHAR                                            AS product_instance_type,
    value['product_instance_type_family']::VARCHAR                                     AS product_instance_type_family,
    value['product_intel_avx2_available']::VARCHAR                                     AS product_intel_avx2_available,
    value['product_intel_avx_available']::VARCHAR                                      AS product_intel_avx_available,
    value['product_intel_turbo_available']::VARCHAR                                    AS product_intel_turbo_available,
    value['product_license_model']::VARCHAR                                            AS product_license_model,
    value['product_location']::VARCHAR                                                 AS product_location,
    value['product_location_type']::VARCHAR                                            AS product_location_type,
    value['product_logs_destination']::VARCHAR                                         AS product_logs_destination,
    value['product_mailbox_storage']::VARCHAR                                          AS product_mailbox_storage,
    value['product_marketoption']::VARCHAR                                             AS product_marketoption,
    value['product_max_iops_burst_performance']::VARCHAR                               AS product_max_iops_burst_performance,
    value['product_max_iopsvolume']::VARCHAR                                           AS product_max_iopsvolume,
    value['product_max_throughputvolume']::VARCHAR                                     AS product_max_throughputvolume,
    value['product_max_volume_size']::VARCHAR                                          AS product_max_volume_size,
    value['product_memory']::VARCHAR                                                   AS product_memory,
    value['product_memory_gib']::VARCHAR                                               AS product_memory_gib,
    value['product_message_delivery_frequency']::VARCHAR                               AS product_message_delivery_frequency,
    value['product_message_delivery_order']::VARCHAR                                   AS product_message_delivery_order,
    value['product_min_volume_size']::VARCHAR                                          AS product_min_volume_size,
    value['product_network_performance']::VARCHAR                                      AS product_network_performance,
    value['product_normalization_size_factor']::VARCHAR                                AS product_normalization_size_factor,
    value['product_operating_system']::VARCHAR                                         AS product_operating_system,
    value['product_operation']::VARCHAR                                                AS product_operation,
    value['product_origin']::VARCHAR                                                   AS product_origin,
    value['product_physical_processor']::VARCHAR                                       AS product_physical_processor,
    value['product_platopricingtype']::VARCHAR                                         AS product_platopricingtype,
    value['product_platousagetype']::VARCHAR                                           AS product_platousagetype,
    value['product_platovolumetype']::VARCHAR                                          AS product_platovolumetype,
    value['product_pre_installed_sw']::VARCHAR                                         AS product_pre_installed_sw,
    value['product_pricing_unit']::VARCHAR                                             AS product_pricing_unit,
    value['product_processor_architecture']::VARCHAR                                   AS product_processor_architecture,
    value['product_processor_features']::VARCHAR                                       AS product_processor_features,
    value['product_product_family']::VARCHAR                                           AS product_product_family,
    value['product_product_name']::VARCHAR                                             AS product_product_name,
    value['product_provisioned']::VARCHAR                                              AS product_provisioned,
    value['product_queue_type']::VARCHAR                                               AS product_queue_type,
    value['product_recipient']::VARCHAR                                                AS product_recipient,
    value['product_region']::VARCHAR                                                   AS product_region,
    value['product_region_code']::VARCHAR                                              AS product_region_code,
    value['product_routing_target']::VARCHAR                                           AS product_routing_target,
    value['product_routing_type']::VARCHAR                                             AS product_routing_type,
    value['product_servicecode']::VARCHAR                                              AS product_servicecode,
    value['product_servicename']::VARCHAR                                              AS product_servicename,
    value['product_sku']::VARCHAR                                                      AS product_sku,
    value['product_storage']::VARCHAR                                                  AS product_storage,
    value['product_storage_class']::VARCHAR                                            AS product_storage_class,
    value['product_storage_media']::VARCHAR                                            AS product_storage_media,
    value['product_tenancy']::VARCHAR                                                  AS product_tenancy,
    value['product_tickettype']::VARCHAR                                               AS product_tickettype,
    value['product_tiertype']::VARCHAR                                                 AS product_tiertype,
    value['product_to_location']::VARCHAR                                              AS product_to_location,
    value['product_to_location_type']::VARCHAR                                         AS product_to_location_type,
    value['product_to_region_code']::VARCHAR                                           AS product_to_region_code,
    value['product_transfer_type']::VARCHAR                                            AS product_transfer_type,
    value['product_usagetype']::VARCHAR                                                AS product_usagetype,
    value['product_vcpu']::VARCHAR                                                     AS product_vcpu,
    value['product_version']::VARCHAR                                                  AS product_version,
    value['product_volume_api_name']::VARCHAR                                          AS product_volume_api_name,
    value['product_volume_type']::VARCHAR                                              AS product_volume_type,
    value['product_vpcnetworkingsupport']::VARCHAR                                     AS product_vpcnetworkingsupport,
    value['reservation_amortized_upfront_cost_for_usage']::FLOAT                       AS reservation_amortized_upfront_cost_for_usage,
    value['reservation_amortized_upfront_fee_for_billing_period']::FLOAT               AS reservation_amortized_upfront_fee_for_billing_period,
    value['reservation_effective_cost']::FLOAT                                         AS reservation_effective_cost,
    TRY_CAST(TO_VARCHAR(value['reservation_end_time']) AS TIMESTAMP)                   AS reservation_end_time,
    value['reservation_modification_status']::VARCHAR                                  AS reservation_modification_status,
    value['reservation_net_amortized_upfront_cost_for_usage']::FLOAT                   AS reservation_net_amortized_upfront_cost_for_usage,
    value['reservation_net_amortized_upfront_fee_for_billing_period']::FLOAT           AS reservation_net_amortized_upfront_fee_for_billing_period,
    value['reservation_net_effective_cost']::FLOAT                                     AS reservation_net_effective_cost,
    value['reservation_net_recurring_fee_for_usage']::FLOAT                            AS reservation_net_recurring_fee_for_usage,
    value['reservation_net_unused_amortized_upfront_fee_for_billing_period']::FLOAT    AS reservation_net_unused_amortized_upfront_fee_for_billing_period,
    value['reservation_net_unused_recurring_fee']::FLOAT                               AS reservation_net_unused_recurring_fee,
    value['reservation_net_upfront_value']::FLOAT                                      AS reservation_net_upfront_value,
    value['reservation_normalized_units_per_reservation']::VARCHAR                     AS reservation_normalized_units_per_reservation,
    value['reservation_number_of_reservations']::VARCHAR                               AS reservation_number_of_reservations,
    value['reservation_recurring_fee_for_usage']::FLOAT                                AS reservation_recurring_fee_for_usage,
    TRY_CAST(TO_VARCHAR(value['reservation_start_time']) AS TIMESTAMP)                 AS reservation_start_time,
    value['reservation_subscription_id']::VARCHAR                                      AS reservation_subscription_id,
    value['reservation_total_reserved_normalized_units']::VARCHAR                      AS reservation_total_reserved_normalized_units,
    value['reservation_total_reserved_units']::VARCHAR                                 AS reservation_total_reserved_units,
    value['reservation_units_per_reservation']::VARCHAR                                AS reservation_units_per_reservation,
    value['reservation_unused_amortized_upfront_fee_for_billing_period']::FLOAT        AS reservation_unused_amortized_upfront_fee_for_billing_period,
    value['reservation_unused_normalized_unit_quantity']::FLOAT                        AS reservation_unused_normalized_unit_quantity,
    value['reservation_unused_quantity']::FLOAT                                        AS reservation_unused_quantity,
    value['reservation_unused_recurring_fee']::FLOAT                                   AS reservation_unused_recurring_fee,
    value['reservation_upfront_value']::FLOAT                                          AS reservation_upfront_value,
    value['savings_plan_amortized_upfront_commitment_for_billing_period']::FLOAT       AS savings_plan_amortized_upfront_commitment_for_billing_period,
    value['savings_plan_net_amortized_upfront_commitment_for_billing_period']::DECIMAL AS savings_plan_net_amortized_upfront_commitment_for_billing_period,
    value['savings_plan_net_recurring_commitment_for_billing_period']::DECIMAL         AS savings_plan_net_recurring_commitment_for_billing_period,
    value['savings_plan_net_savings_plan_effective_cost']::DECIMAL                     AS savings_plan_net_savings_plan_effective_cost,
    value['savings_plan_recurring_commitment_for_billing_period']::DECIMAL             AS savings_plan_recurring_commitment_for_billing_period,
    value['savings_plan_savings_plan_a_r_n']::VARCHAR                                  AS savings_plan_savings_plan_a_r_n,
    value['savings_plan_savings_plan_effective_cost']::DECIMAL                         AS savings_plan_savings_plan_effective_cost,
    value['savings_plan_savings_plan_rate']::DECIMAL                                   AS savings_plan_savings_plan_rate,
    value['savings_plan_total_commitment_to_date']::DECIMAL                            AS savings_plan_total_commitment_to_date,
    value['savings_plan_used_commitment']::DECIMAL                                     AS savings_plan_used_commitment,
    modified_at_                                                                       AS modified_at
  FROM all_raw
),

unique_ids AS (

  SELECT DISTINCT
    identity_line_item_id,
    identity_time_interval
  FROM parsed
),

filtered AS (

  SELECT parsed.*
  FROM parsed
  INNER JOIN unique_ids ON parsed.identity_line_item_id = unique_ids.identity_line_item_id
    AND parsed.identity_time_interval = unique_ids.identity_time_interval
)

SELECT * FROM filtered