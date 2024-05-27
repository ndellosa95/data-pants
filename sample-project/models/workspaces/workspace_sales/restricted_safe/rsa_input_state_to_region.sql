{{
    config(
        materialized='table'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_input_state_to_region_source') }}

)
SELECT *
FROM source