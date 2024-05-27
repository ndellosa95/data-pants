WITH issue_notes AS (
SELECT
    id AS note_id
    , created_at AS note_created_at
    , updated_at AS note_updated_at
    , author_id AS note_author_id
    , noteable_id AS note_issue_id
    , REGEXP_SUBSTR_ALL(note_html, 'data-merge-request="(\\d+)', 1, 1, 'e', 1) referenced_merge_request_id_array
    , REGEXP_SUBSTR_ALL(note_html, 'data-issue="(\\d+)', 1, 1, 'e', 1) referenced_issue_id_array
    , REGEXP_SUBSTR_ALL(note_html, 'data-epic="(\\d+)', 1, 1, 'e', 1) referenced_epic_id_array
    , IFF(REGEXP_SUBSTR(note_html, 'data-merge-request="(\\d+)', 1, 1, 'e', 1) is not null, 1, 0)::boolean AS is_merge_request_reference
    , IFF(REGEXP_SUBSTR(note_html, 'data-issue="(\\d+)', 1, 1, 'e', 1) is not null, 1, 0)::boolean AS is_issue_reference
    , IFF(REGEXP_SUBSTR(note_html, 'data-epic="(\\d+)', 1, 1, 'e', 1) is not null, 1, 0)::boolean AS is_epic_reference
FROM
    {{ ref('gitlab_dotcom_notes_dedupe_source') }}
WHERE
    noteable_type = 'Issue'
    AND (REGEXP_SUBSTR(note_html, 'data-merge-request="(\\d+)', 1, 1, 'e', 1) is not null
    OR REGEXP_SUBSTR(note_html, 'data-issue="(\\d+)', 1, 1, 'e', 1) is not null
    OR REGEXP_SUBSTR(note_html, 'data-epic="(\\d+)', 1, 1, 'e', 1) is not null)
)

SELECT
    *
FROM
    issue_notes
