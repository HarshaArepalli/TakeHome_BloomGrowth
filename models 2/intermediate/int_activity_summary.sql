-- Summarize activity metrics by category and organization
with activity_events as (
    select * from {{ ref('stg_activity_events') }}
),
feature_usage as (
    select
        organization_id,
        feature_category,
        count(distinct event_id) as event_count,
        count(distinct user_id) as user_count,
        count(distinct event_date) as usage_days,
        min(event_date) as first_usage_date,
        max(event_date) as last_usage_date
    from activity_events
    group by organization_id, feature_category
),
org_totals as (
    select
        organization_id,
        sum(event_count) as total_org_events
    from feature_usage
    group by organization_id
)
select
    fu.organization_id,
    fu.feature_category,
    fu.event_count,
    fu.user_count,
    fu.usage_days,
    fu.first_usage_date,
    fu.last_usage_date,
    round(100.0 * fu.event_count / ot.total_org_events, 2) as pct_of_org_activity
from feature_usage fu
left join org_totals ot on fu.organization_id = ot.organization_id
order by fu.organization_id, fu.event_count desc
