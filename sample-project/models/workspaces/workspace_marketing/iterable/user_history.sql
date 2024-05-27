WITH source AS (

    SELECT *
    FROM {{ ref('user_history_source') }}

)

SELECT *
FROM source