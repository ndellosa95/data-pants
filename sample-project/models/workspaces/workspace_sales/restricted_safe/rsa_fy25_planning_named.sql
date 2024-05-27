{{
    config(
        materialized='table'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('rsa_input_fy25_planning_named_source') }}

)
SELECT *
FROM source