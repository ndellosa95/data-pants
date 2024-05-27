WITH
source AS (
  SELECT * FROM
    {{ source('level_up', 'course_views') }}
),

intermediate AS (
  SELECT
    data.value,
    source.uploaded_at
  FROM
    source
  INNER JOIN LATERAL FLATTEN(input => source.jsontext['data']) AS data
),

parsed AS (
  SELECT
    value['companyId']::VARCHAR                 AS company_id,
    value['courseId']::VARCHAR                  AS course_id,
    value['courseSku']::VARCHAR                 AS course_sku,
    value['courseTitle']::VARCHAR               AS course_title,
    value['timestamp']::TIMESTAMP               AS event_timestamp,
    value['lessonId']::VARCHAR                  AS lesson_id,
    value['lessonSlug']::VARCHAR                AS lesson_slug,
    value['lessonTitle']::VARCHAR               AS lesson_title,
    value['sectionId']::VARCHAR                 AS section_id,
    value['sectionSlug']::VARCHAR               AS section_slug,
    value['sectionTitle']::VARCHAR              AS section_title,
    value['topicId']::VARCHAR                   AS topic_id,
    value['topicTitle']::VARCHAR                AS topic_title,

    CASE
      WHEN LOWER(value['user']) ILIKE '%@gitlab.com' THEN value['user']::VARCHAR
    END                                         AS username,
    value['userDetail']['id']::VARCHAR          AS user_id,

    value['userDetail']['state']::VARCHAR       AS user_state,
    value['userDetail']['country']::VARCHAR     AS user_country,

    value['userDetail']['ref1']::VARCHAR        AS ref1_user_type,
    value['userDetail']['ref2']::VARCHAR        AS ref2_user_job,
    value['userDetail']['ref4']::VARCHAR        AS ref4_user_company,
    value['userDetail']['ref6']::VARCHAR        AS ref6_user_role_type,
    value['userDetail']['ref7']::VARCHAR        AS ref7_user_continent,
    value['userDetail']['ref8']::VARCHAR        AS ref8_user_country,
    value['userDetail']['ref9']::VARCHAR        AS ref9_user_sub_dept,
    value['userDetail']['ref10']::VARCHAR       AS ref10_user_dept,

    value['userDetail']['sfAccountId']::VARCHAR AS sf_account_id,
    value['userDetail']['sfContactId']::VARCHAR AS sf_contact_id,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id,
        event_timestamp,
        topic_title
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
