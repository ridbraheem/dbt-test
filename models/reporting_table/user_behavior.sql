WITH cte_start as (
    SELECT
        d_users.user_id,
        user_actives.active_last_14,
        user_actives.active_last_30,
        user_session.first_touch_date, 
        user_session.session_count, 
        user_session.total_touches, 
        user_session.first_touch_url
    FROM
        {{ ref('dim_users') }} d_users
    LEFT JOIN {{ ref('agg_user_sessions') }} user_session ON user_session.user_id = d_users.user_id
    LEFT JOIN {{ ref('agg_user_actives') }} user_actives ON user_actives.user_id = d_users.user_id
)
SELECT
    user_id,
    IFNULL(active_last_14, 'False') as active_last_14,
    IFNULL(active_last_30, 'False') as active_last_30,
    first_touch_date, 
    session_count, 
    total_touches, 
    first_touch_url
FROM cte_start