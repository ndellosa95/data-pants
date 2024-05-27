WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','user_history') }}
 
), final AS (
 
    SELECT
        updated_at::TIMESTAMP                        AS updated_at,
        first_name::VARCHAR                          AS first_name,
        last_name::VARCHAR                           AS last_name,
        phone_number::VARCHAR                        AS phone_number,
        user_id::VARCHAR                             AS user_id,
        signup_date::TIMESTAMP                       AS signup_date,
        signup_source::VARCHAR                       AS signup_source,
        email_list_ids::VARIANT                      AS email_list_ids,
        email::VARCHAR                               AS email,
        phone_number_carrier::VARCHAR                AS phone_number_carrier,
        phone_number_country_code_iso::VARCHAR       AS phone_number_country_code_iso,
        phone_number_line_type::VARCHAR              AS phone_number_line_type,
        phone_number_updated_at::VARCHAR             AS phone_number_updated_at,
        iterable_user_id::VARCHAR                    AS iterable_user_id,
        additional_properties::VARIANT               AS additional_properties
    FROM source
)

SELECT *
FROM final
