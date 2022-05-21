WITH cte_start as (
    SELECT 
   replace(
       replace(
            replace(data,'""','"')
       , '"{', '{'),
       '}"', '}')as d,
FROM
   {{ source('raw', 'users') }}
   )
--cte_start, UNNEST(SPLIT(d)) as splitted
SELECT  *,
JSON_EXTRACT_SCALAR(d , "$.defaultOrganization") AS defaultOrganization ,
JSON_EXTRACT_SCALAR(d , "$.displayName") AS displayName ,
JSON_EXTRACT_SCALAR(d , "$.enabled") AS enabled ,
JSON_EXTRACT_SCALAR(d , "$.user_id")  AS user_id
FROM cte_start
--cte_start, UNNEST(SPLIT(d)) as splitted

--SELECT "1234" AS row_id, '{ "id" : "123" , "items" : [ { "quantity" : 1 , "product" : { "id" : "p1" , "categories" : [ "cat1","cat2","cat3"]  }}] }' AS parts

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