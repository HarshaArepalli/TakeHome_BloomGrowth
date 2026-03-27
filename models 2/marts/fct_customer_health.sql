-- Customer health scoring and churn risk assessment
with org_metrics as (
    select * from {{ ref('int_organization_metrics') }}
),
engagement_timeline as (
    select * from {{ ref('int_engagement_timeline') }}
),
user_engagement as (
    select * from {{ ref('int_user_engagement') }}
),
latest_engagement as (
    select
        organization_id,
        max(event_date) as last_engagement_date,
        max(daily_event_count) as max_daily_events,
        avg(daily_event_count) as avg_daily_events
    from engagement_timeline
    group by organization_id
),
user_health as (
    select
        organization_id,
        count(distinct case when activity_status = 'active' then user_id end) as active_user_count,
        count(distinct case when activity_status = 'at_risk' then user_id end) as at_risk_user_count,
        count(distinct case when activity_status = 'dormant' then user_id end) as dormant_user_count,
        avg(engagement_score) as avg_engagement_score
    from user_engagement
    group by organization_id
)
select
    om.organization_id,
    om.organization_name,
    om.business_os_type,
    om.industry,
    om.status,
    om.current_mrr,
    om.max_mrr_ever,
    om.total_events,
    om.active_users,
    om.categories_used,
    om.total_users,
    om.registered_users,
    le.last_engagement_date,
    le.avg_daily_events,
    uh.active_user_count,
    uh.at_risk_user_count,
    uh.dormant_user_count,
    uh.avg_engagement_score,
    -- Churn risk scoring (0-100, higher = more risk)
    case
        when om.status = 'churned' then 100
        when om.current_mrr = 0 then 95
        when om.current_mrr < om.max_mrr_ever * 0.5 then 80
        when om.downgrade_count > om.upgrade_count then 70
        when datediff('day', le.last_engagement_date, current_date) > 30 then 65
        when uh.dormant_user_count > uh.active_user_count * 0.5 then 60
        when uh.avg_engagement_score < 2 then 50
        when datediff('day', le.last_engagement_date, current_date) > 14 then 40
        else 20
    end as churn_risk_score,
    -- Health score (0-100, higher = healthier)
    case
        when om.status = 'churned' then 0
        when om.current_mrr = 0 then 5
        when om.upgrade_count > 0 and le.avg_daily_events > 10 then 85
        when om.upgrade_count > 0 or (uh.active_user_count > uh.dormant_user_count and le.avg_daily_events > 5) then 75
        when uh.avg_engagement_score >= 3 and datediff('day', le.last_engagement_date, current_date) < 14 then 70
        when le.avg_daily_events > 5 then 60
        when uh.avg_engagement_score >= 2 then 50
        else 30
    end as health_score
from org_metrics om
left join latest_engagement le on om.organization_id = le.organization_id
left join user_health uh on om.organization_id = uh.organization_id
order by churn_risk_score desc, health_score desc
