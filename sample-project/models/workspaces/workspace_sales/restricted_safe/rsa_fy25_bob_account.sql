{{
    config(
        materialized='table'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_fy25_bob_account_source') }}

)
SELECT *
FROM source