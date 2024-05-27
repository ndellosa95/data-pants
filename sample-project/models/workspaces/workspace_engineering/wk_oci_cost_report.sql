WITH source AS (

  SELECT *
  FROM {{ ref('oci_cost_report_source') }}

)

SELECT *
FROM source
