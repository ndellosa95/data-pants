WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','user_unsubscribed_channel') }}
 
), final AS (
 
    SELECT   
      channel_id::NUMBER AS channel_id
    FROM source
)

SELECT *
FROM final
