WITH cte_start as ( 
    SELECT 
        *
    FROM
   {{ source('raw', 'pageviews') }}
),
cte_user_activities_agg as (
    SELECT 
        users,
        COUNT(DISTINCT anonymous_id) as session_count,
        COUNT(*) as total_touches
    FROM
        cte_start
    GROUP BY users
), 
cte_rank_session as (
    SELECT
        users,
        DATE(timestamp) as session_date,
        url,
        ROW_NUMBER() OVER (PARTITION BY users ORDER BY timestamp asc) RowNumber
    FROM
        cte_start
),
cte_first_session as (
    SELECT 
        users,
        session_date as first_touch_date,
        url as first_touch_url
    FROM 
        cte_rank_session
    WHERE
        RowNumber = 1
)
SELECT
    cte_user_activities_agg.users as user_id,
    cte_user_activities_agg.session_count,
    cte_user_activities_agg.total_touches,
    cte_first_session.first_touch_date,
    cte_first_session.first_touch_url
FROM 
    cte_user_activities_agg 
    LEFT JOIN cte_first_session ON cte_user_activities_agg.users = cte_first_session.users



