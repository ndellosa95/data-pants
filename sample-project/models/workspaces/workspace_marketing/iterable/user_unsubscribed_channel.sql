WITH source AS (

    SELECT *
    FROM {{ ref('user_unsubscribed_channel_source') }}

)

SELECT *
FROM source