{{ config(
    tags=["mnpi","gainsight"]
) }}

WITH source AS (

    SELECT
    {{ hash_sensitive_columns('email_logs') }}
    FROM {{ ref('email_logs') }}

)

SELECT *
FROM source