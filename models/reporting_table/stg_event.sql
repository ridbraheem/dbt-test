-- Staging table for events and users

WITH cte_start as (
    SELECT 
        document_id,
        event_id,
        operation,
        REPLACE(
            REPLACE(
                REPLACE(data,'""','"')
        , '"{', '{'),
        '}"', '}') as d
    FROM
   {{ source('raw', 'users') }}
   )
SELECT 
    document_id,
    event_id,
    operation,
    JSON_EXTRACT_SCALAR(d , "$.defaultOrganization") AS defaultOrganization ,
    JSON_EXTRACT_SCALAR(d , "$.displayName") AS displayName ,
    JSON_EXTRACT_SCALAR(d , "$.enabled") AS enabled ,
    JSON_EXTRACT_SCALAR(d , "$.user_id")  AS user_id,
    JSON_EXTRACT_SCALAR(d , "$.address.active")  AS active,
    JSON_EXTRACT_SCALAR(d , "$.address.address1")  AS address1,
    JSON_EXTRACT_SCALAR(d , "$.address.address2")  AS address2,
    JSON_EXTRACT_SCALAR(d , "$.address.capacityGoal")  AS capacityGoal,
    JSON_EXTRACT_SCALAR(d , "$.address.city")  AS city
FROM cte_start

--pageviews
    --pages_id
    --anonymous_id
    --users
    --admin
    --sent_at
    --timestamp
    --path_array
    --path_depth
    --path_array
    --title
    --url
    --level_1
--users
    --document_id
    --event_id
    --operation
    --data

-- user_behavior which contains one row per user uuid.
-- active_last_14, active_last_30, first_touch_date, session_count, total_touches, first_touch_url.