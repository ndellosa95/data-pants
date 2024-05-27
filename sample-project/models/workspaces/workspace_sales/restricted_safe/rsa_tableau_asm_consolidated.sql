{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_tableau_asm_consolidated_source') }}

)
SELECT *
FROM source