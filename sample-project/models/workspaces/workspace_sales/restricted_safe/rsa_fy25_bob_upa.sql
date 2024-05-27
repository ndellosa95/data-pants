{{
    config(
        materialized='table'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_fy25_bob_upa_source') }}

)
SELECT *
FROM source