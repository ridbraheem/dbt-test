    -- using max date as current date
    
WITH cte_start as (
    SELECT 
        *
    FROM
        {{ source('raw', 'pageviews') }}
),
cte_dates as (
    SELECT 
        MAX(DATE(timestamp)) as current_date,
        MAX(DATE(timestamp))-14 as fourteen_days_ago,
        MAX(DATE(timestamp))-30 as thirty_days_ago
    FROM
        cte_start
),
cte_users as (
    SELECT 
        DISTINCT users
    FROM
        cte_start
),
active_14_days as (
    SELECT 
        DISTINCT users
    FROM
        cte_start 
    WHERE
        DATE(timestamp) BETWEEN (SELECT fourteen_days_ago FROM cte_dates) AND (SELECT current_date FROM cte_dates)
),
active_30_days as (
    SELECT 
        DISTINCT users
    FROM
        cte_start 
    WHERE
        DATE(timestamp) BETWEEN (SELECT thirty_days_ago FROM cte_dates) AND (SELECT current_date FROM cte_dates)
)
SELECT
    cte_users.users as user_id,
    active_14_days.users as active_14_user_id,
    active_30_days.users as active_30_user_id,
    IF(active_14_days.users is null, 'False', 'True') as active_last_14,
    IF(active_30_days.users is null, 'False', 'True') as active_last_30
FROM
    cte_users
    LEFT JOIN active_14_days ON active_14_days.users = cte_users.users
    LEFT JOIN active_30_days ON active_30_days.users = cte_users.users
