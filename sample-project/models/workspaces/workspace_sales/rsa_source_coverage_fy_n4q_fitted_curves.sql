{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('ssa_fy_n4q_coverage_fitted_curves_source') }}

)
SELECT *
FROM source