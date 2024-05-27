    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_alert_management_alerts_dedupe_source') }}
    
), renamed AS (

    SELECT
        id::NUMBER                AS alert_management_alert_id,
        created_at::TIMESTAMP     AS created_at,
        updated_at::TIMESTAMP     AS updated_at,
        started_at::TIMESTAMP     AS started_at,
        ended_at::TIMESTAMP       AS ended_at,
        events::NUMBER            AS alert_management_alert_events,
        iid::NUMBER               AS alert_management_alert_iid,
        status::NUMBER            AS status_id,
        issue_id::NUMBER          AS issue_id,
        project_id::NUMBER        AS project_id

    FROM source

)

SELECT *
FROM renamed
