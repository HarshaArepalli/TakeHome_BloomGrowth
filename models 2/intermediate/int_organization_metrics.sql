-- Aggregate organization-level metrics
with organizations as (
    select * from {{ ref('stg_organizations') }}
),
subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),
activity_events as (
    select * from {{ ref('stg_activity_events') }}
),
users as (
    select * from {{ ref('stg_users') }}
),
org_subscription_metrics as (
    select
        o.organization_id,
        o.organization_name,
        o.business_os_type,
        o.industry,
        o.status,
        count(distinct s.subscription_id) as total_subscriptions,
        max(case when s.subscription_status = 'active' then s.started_at end) as latest_subscription_start,
        max(s.started_at) as max_subscription_date,
        sum(case when s.subscription_status = 'active' then s.mrr_amount else 0 end) as current_mrr,
        max(s.mrr_amount) as max_mrr_ever,
        count(distinct case when s.change_reason = 'upgrade' then s.subscription_id end) as upgrade_count,
        count(distinct case when s.change_reason = 'downgrade' then s.subscription_id end) as downgrade_count,
        count(distinct case when s.change_reason = 'churn' then s.subscription_id end) as churn_count
    from organizations o
    left join subscriptions s on o.organization_id = s.organization_id
    group by o.organization_id, o.organization_name, o.business_os_type, o.industry, o.status
),
activity_metrics as (
    select
        organization_id,
        count(distinct event_id) as total_events,
        count(distinct user_id) as active_users,
        count(distinct feature_category) as categories_used,
        max(event_date) as last_event_date
    from activity_events
    group by organization_id
),
user_metrics as (
    select
        organization_id,
        count(distinct user_id) as total_users,
        sum(case when is_registered = true then 1 else 0 end) as registered_users,
        count(distinct case when role = 'owner' then user_id end) as owner_count,
        count(distinct case when role = 'admin' then user_id end) as admin_count
    from users
    group by organization_id
)
select
    osm.organization_id,
    osm.organization_name,
    osm.business_os_type,
    osm.industry,
    osm.status,
    osm.total_subscriptions,
    osm.current_mrr,
    osm.max_mrr_ever,
    osm.upgrade_count,
    osm.downgrade_count,
    osm.churn_count,
    coalesce(am.total_events, 0) as total_events,
    coalesce(am.active_users, 0) as active_users,
    coalesce(am.categories_used, 0) as categories_used,
    am.last_event_date,
    coalesce(um.total_users, 0) as total_users,
    coalesce(um.registered_users, 0) as registered_users,
    coalesce(um.owner_count, 0) as owner_count,
    coalesce(um.admin_count, 0) as admin_count
from org_subscription_metrics osm
left join activity_metrics am on osm.organization_id = am.organization_id
left join user_metrics um on osm.organization_id = um.organization_id
