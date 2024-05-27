{{ config(
    materialized='view',
    tags=["mnpi_exception"]
) }}

WITH seat_link AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['hostname', 'uuid']) }} AS latest_seat_link_installation_sk,
      zuora_subscription_id AS dim_subscription_id,
      zuora_subscription_name AS subscription_name,
      hostname AS host_name,
      uuid AS dim_instance_id,
      order_id,
      report_timestamp,
      report_date,
      license_starts_on,
      created_at,
      updated_at,
      active_user_count,
      license_user_count,
      max_historical_user_count
    FROM {{ ref('customers_db_license_seat_links_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY host_name, dim_instance_id ORDER BY report_date DESC, updated_at DESC) = 1

)

{{ dbt_audit(
    cte_ref="seat_link",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2024-03-06",
    updated_date="2024-04-18"
) }}
