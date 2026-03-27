-- Calculate user engagement scores
with users as (
    select * from {{ ref('stg_users') }}
),
activity_events as (
    select * from {{ ref('stg_activity_events') }}
),
user_activity as (
    select
        ae.user_id,
        ae.organization_id,
        count(distinct ae.event_id) as total_events,
        count(distinct ae.feature_category) as features_used,
        count(distinct ae.event_date) as active_days,
        max(ae.event_date) as last_active_date,
        min(ae.event_date) as first_event_date
    from activity_events ae
    group by ae.user_id, ae.organization_id
)
select
    u.user_id,
    u.organization_id,
    u.user_name,
    u.role,
    u.is_registered,
    u.created_at,
    u.last_active_at,
    coalesce(ua.total_events, 0) as total_events,
    coalesce(ua.features_used, 0) as features_used,
    coalesce(ua.active_days, 0) as active_days,
    ua.last_active_date,
    ua.first_event_date,
    case
        when ua.total_events = 0 then 0
        when ua.total_events < 5 then 1
        when ua.total_events < 20 then 2
        when ua.total_events < 50 then 3
        else 4
    end as engagement_score,
    case
        when ua.last_active_date is null then 'inactive'
        when datediff('day', ua.last_active_date, current_date) > 30 then 'dormant'
        when datediff('day', ua.last_active_date, current_date) > 7 then 'at_risk'
        else 'active'
    end as activity_status
from users u
left join user_activity ua on u.user_id = ua.user_id and u.organization_id = ua.organization_id
