{{
    config(
        materialized='table'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_input_industry_to_industry_category_source') }}

)
SELECT *
FROM source