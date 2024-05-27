WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_trial_histories') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY gl_namespace_id, trial_type ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT DISTINCT
      TRY_TO_NUMERIC(gl_namespace_id)::VARCHAR  AS gl_namespace_id,
      start_date::TIMESTAMP                     AS start_date,
      expired_on::TIMESTAMP                     AS expired_on,
      created_at::TIMESTAMP                     AS created_at,
      updated_at::TIMESTAMP                     AS updated_at,
      glm_source::VARCHAR                       AS glm_source,
      glm_content::VARCHAR                      AS glm_content,
      trial_entity::VARCHAR                     AS trial_entity,
      trial_type::INTEGER                       AS trial_type,
      CASE
        WHEN trial_type::INTEGER = 1 THEN 'Ultimate/Premium'
        WHEN trial_type::INTEGER = 2 THEN 'DuoPro'
        ELSE NULL
      END AS trial_type_name
    FROM source


)

SELECT *
FROM renamed
WHERE gl_namespace_id IS NOT NULL
