WITH base as (SELECT *
              FROM {{ source('gitlab_dotcom', 'service_desk_settings') }}
)

SELECT *
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY _uploaded_at DESC) = 1
