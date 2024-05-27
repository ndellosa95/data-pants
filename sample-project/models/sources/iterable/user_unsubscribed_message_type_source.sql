WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','user_unsubscribed_message_type') }}
 
), final AS (
 
    SELECT   
      message_type_id::NUMBER AS message_type_id
    FROM source
)

SELECT *
FROM final
