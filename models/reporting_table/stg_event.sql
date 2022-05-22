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
    JSON_EXTRACT_SCALAR(d , "$.address.city")  AS city,
    d as json_string
FROM cte_start

