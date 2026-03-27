-- Analyze engagement patterns and activity trends
with activity_events as (
    select * from {{ ref('stg_activity_events') }}
),
date_spine as (
    -- Create date spine for daily activity tracking
    select distinct event_date from activity_events
),
daily_activity as (
    select
        organization_id,
        event_date,
        count(distinct event_id) as daily_event_count,
        count(distinct user_id) as daily_active_users,
        count(distinct feature_category) as daily_categories_used
    from activity_events
    group by organization_id, event_date
),
weekly_activity as (
    select
        organization_id,
        date_trunc('week', event_date) as week_start,
        count(distinct event_id) as weekly_events,
        count(distinct user_id) as weekly_active_users,
        count(distinct feature_category) as weekly_categories_used
    from activity_events
    group by organization_id, date_trunc('week', event_date)
),
monthly_activity as (
    select
        organization_id,
        date_trunc('month', event_date) as month_start,
        count(distinct event_id) as monthly_events,
        count(distinct user_id) as monthly_active_users,
        count(distinct feature_category) as monthly_categories_used
    from activity_events
    group by organization_id, date_trunc('month', event_date)
)
select
    da.organization_id,
    da.event_date,
    da.daily_event_count,
    da.daily_active_users,
    da.daily_categories_used,
    wa.weekly_events,
    wa.weekly_active_users,
    wa.weekly_categories_used,
    ma.monthly_events,
    ma.monthly_active_users,
    ma.monthly_categories_used
from daily_activity da
left join weekly_activity wa on da.organization_id = wa.organization_id 
    and date_trunc('week', da.event_date) = wa.week_start
left join monthly_activity ma on da.organization_id = ma.organization_id 
    and date_trunc('month', da.event_date) = ma.month_start
order by da.organization_id, da.event_date
