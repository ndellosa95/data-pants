WITH base AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'catalog_resources') }}

)

{{ scd_latest_state(source='base', max_column='_task_instance') }}
