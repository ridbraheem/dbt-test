WITH cte_start as (
    SELECT 
        DISTINCT
        CAST(user_id AS NUMERIC) as user_id,
        displayName as user_name,
        active,
        address1,
        address2,
        city
    FROM
   {{ ref('stg_event') }}
   )
SELECT
    *
FROM cte_start