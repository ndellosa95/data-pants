WITH source AS (

    SELECT *
    FROM {{ ref('rsa_ae_quotas_unpivoted_source') }}

)
SELECT *
FROM source