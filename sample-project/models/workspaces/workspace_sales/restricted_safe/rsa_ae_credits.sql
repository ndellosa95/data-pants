{{
    config(
        materialized='table'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_ae_credits_source') }}

)
SELECT *
FROM source