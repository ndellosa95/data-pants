{{
    config(
        materialized='view'
    )
}}

WITH source AS (

    SELECT *
    FROM {{ ref('ssa_coverage_fitted_curves_source') }}

)
SELECT *
FROM source