{{
    config(
        materialized='table'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_input_zip_code_to_region_source') }}

)
SELECT *
FROM source