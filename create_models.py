#!/usr/bin/env python3
"""
Generate dbt models by executing SQL queries in DuckDB and exporting results.
"""
import duckdb
import os

db_path = "/Users/shivachaithanyagoli/Desktop/harsha_interview_assessment/analytics.duckdb"
conn = duckdb.connect(db_path)
models_dir = "/Users/shivachaithanyagoli/Desktop/harsha_interview_assessment/models"

# Create the staging layer views and tables
print("Creating staging layer models...")

# stg_organizations
stg_organizations = """
CREATE OR REPLACE VIEW stg_organizations AS
with source_data as (
    select
        organization_id,
        organization_name,
        business_os_type,
        industry,
        employee_count,
        status,
        created_at,
        churned_at
    from organizations
)
select
    organization_id,
    organization_name,
    case
        when business_os_type in ('EOS', 'Pinnacle', 'Scaling Up', 'Custom') then business_os_type
        else 'Custom'
    end as business_os_type,
    industry,
    coalesce(employee_count, 0) as employee_count,
    case
        when status in ('active', 'churned', 'trial') then status
        else 'trial'
    end as status,
    created_at,
    case
        when status = 'churned' and churned_at is null then created_at
        else churned_at
    end as churned_at
from source_data;
"""

stg_plans = """
CREATE OR REPLACE VIEW stg_plans AS
select
    plan_id,
    plan_name,
    coalesce(base_price, 0) as base_price,
    coalesce(per_seat_price, 0) as per_seat_price
from plans;
"""

stg_subscriptions = """
CREATE OR REPLACE VIEW stg_subscriptions AS
with source_data as (
    select
        subscription_id,
        organization_id,
        plan_id,
        seats,
        mrr_amount,
        billing_interval,
        started_at,
        ended_at,
        change_reason
    from subscriptions
)
select
    subscription_id,
    organization_id,
    plan_id,
    coalesce(seats, 1) as seats,
    coalesce(mrr_amount, 0) as mrr_amount,
    case
        when billing_interval in ('monthly', 'annual') then billing_interval
        else 'monthly'
    end as billing_interval,
    started_at,
    case
        when trim(cast(ended_at as varchar)) = '' or trim(cast(ended_at as varchar)) is null then null
        else try_cast(ended_at as date)
    end as ended_at,
    case
        when change_reason in ('new', 'upgrade', 'downgrade', 'churn', 'reactivation') then change_reason
        else 'new'
    end as change_reason,
    case when trim(cast(ended_at as varchar)) != '' and trim(cast(ended_at as varchar)) is not null then 'churned' else 'active' end as subscription_status
from source_data
order by organization_id, started_at;
"""

stg_users = """
CREATE OR REPLACE VIEW stg_users AS
select
    user_id,
    organization_id,
    user_name,
    case
        when role in ('owner', 'admin', 'manager', 'member') then role
        else 'member'
    end as role,
    coalesce(is_registered, false) as is_registered,
    created_at,
    last_active_at
from users;
"""

stg_activity_events = """
CREATE OR REPLACE VIEW stg_activity_events AS
select
    event_id,
    organization_id,
    user_id,
    case
        when feature_category in ('meetings', 'scorecards', 'rocks', 'todos', 'issues', 'headlines') 
            then feature_category
        else 'other'
    end as feature_category,
    event_type,
    event_date,
    event_timestamp
from activity_events
order by event_timestamp;
"""

for sql in [stg_organizations, stg_plans, stg_subscriptions, stg_users, stg_activity_events]:
    conn.execute(sql)
    print("  ✓ Created staging view")

# Create intermediate layer models
print("\nCreating intermediate layer models...")

int_organization_metrics = """
CREATE OR REPLACE TABLE int_organization_metrics AS
with organizations as (
    select * from stg_organizations
),
subscriptions as (
    select * from stg_subscriptions
),
activity_events as (
    select * from stg_activity_events
),
users as (
    select * from stg_users
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
left join user_metrics um on osm.organization_id = um.organization_id;
"""

int_subscription_changes = """
CREATE OR REPLACE TABLE int_subscription_changes AS
with subscriptions as (
    select * from stg_subscriptions
),
plans as (
    select * from stg_plans
),
subscription_changes as (
    select
        s.subscription_id,
        s.organization_id,
        s.plan_id,
        p.plan_name,
        s.seats,
        s.mrr_amount,
        s.started_at,
        s.ended_at,
        s.change_reason,
        lag(s.mrr_amount) over (partition by s.organization_id order by s.started_at) as previous_mrr,
        s.mrr_amount - lag(s.mrr_amount) over (partition by s.organization_id order by s.started_at) as mrr_change,
        case 
            when s.change_reason = 'upgrade' then 'expansion'
            when s.change_reason = 'downgrade' then 'contraction'
            when s.change_reason = 'churn' then 'churn'
            when s.change_reason = 'reactivation' then 'reactivation'
            else 'new'
        end as change_type
    from subscriptions s
    left join plans p on s.plan_id = p.plan_id
)
select
    subscription_id,
    organization_id,
    plan_id,
    plan_name,
    seats,
    mrr_amount,
    previous_mrr,
    mrr_change,
    started_at,
    ended_at,
    change_reason,
    change_type,
    case 
        when ended_at is not null then datediff('day', started_at, ended_at)
        else datediff('day', started_at, CURRENT_DATE)
    end as subscription_days
from subscription_changes
order by organization_id, started_at;
"""

int_user_engagement = """
CREATE OR REPLACE TABLE int_user_engagement AS
with users as (
    select * from stg_users
),
activity_events as (
    select * from stg_activity_events
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
        when datediff('day', ua.last_active_date, CURRENT_DATE) > 30 then 'dormant'
        when datediff('day', ua.last_active_date, CURRENT_DATE) > 7 then 'at_risk'
        else 'active'
    end as activity_status
from users u
left join user_activity ua on u.user_id = ua.user_id and u.organization_id = ua.organization_id;
"""

int_activity_summary = """
CREATE OR REPLACE TABLE int_activity_summary AS
with activity_events as (
    select * from stg_activity_events
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
order by fu.organization_id, fu.event_count desc;
"""

for sql in [int_organization_metrics, int_subscription_changes, int_user_engagement, int_activity_summary]:
    conn.execute(sql)
    print("  ✓ Created intermediate table")

# Create mart layer models for analysis
print("\nCreating mart layer models...")

fct_customer_health = """
CREATE OR REPLACE TABLE fct_customer_health AS
with org_metrics as (
    select * from int_organization_metrics
),
latest_engagement as (
    select
        organization_id,
        max(event_date) as last_event_date
    from stg_activity_events
    group by organization_id
),
daily_activity as (
    select
        organization_id,
        event_date,
        count(distinct event_id) as daily_events
    from stg_activity_events
    group by organization_id, event_date
),
org_daily_stats as (
    select
        organization_id,
        max(daily_events) as max_daily_events,
        avg(daily_events) as avg_daily_events
    from daily_activity
    group by organization_id
),
user_health as (
    select
        organization_id,
        count(distinct case when activity_status = 'active' then user_id end) as active_user_count,
        count(distinct case when activity_status = 'at_risk' then user_id end) as at_risk_user_count,
        count(distinct case when activity_status = 'dormant' then user_id end) as dormant_user_count,
        avg(engagement_score) as avg_engagement_score
    from int_user_engagement
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
    le.last_event_date,
    ods.avg_daily_events,
    uh.active_user_count,
    uh.at_risk_user_count,
    uh.dormant_user_count,
    uh.avg_engagement_score,
    case
        when om.status = 'churned' then 100
        when om.current_mrr = 0 then 95
        when om.current_mrr < om.max_mrr_ever * 0.5 then 80
        when om.downgrade_count > om.upgrade_count then 70
        when datediff('day', le.last_event_date, CURRENT_DATE) > 30 then 65
        when uh.dormant_user_count > uh.active_user_count * 0.5 then 60
        when uh.avg_engagement_score < 2 then 50
        when datediff('day', le.last_event_date, CURRENT_DATE) > 14 then 40
        else 20
    end as churn_risk_score,
    case
        when om.status = 'churned' then 0
        when om.current_mrr = 0 then 5
        when om.upgrade_count > 0 and ods.avg_daily_events > 10 then 85
        when om.upgrade_count > 0 or (uh.active_user_count > uh.dormant_user_count and ods.avg_daily_events > 5) then 75
        when uh.avg_engagement_score >= 3 and datediff('day', le.last_event_date, CURRENT_DATE) < 14 then 70
        when ods.avg_daily_events > 5 then 60
        when uh.avg_engagement_score >= 2 then 50
        else 30
    end as health_score
from org_metrics om
left join latest_engagement le on om.organization_id = le.organization_id
left join org_daily_stats ods on om.organization_id = ods.organization_id
left join user_health uh on om.organization_id = uh.organization_id;
"""

fct_revenue_metrics = """
CREATE OR REPLACE TABLE fct_revenue_metrics AS
with org_metrics as (
    select * from int_organization_metrics
),
subscription_changes as (
    select * from int_subscription_changes
),
subscription_summary as (
    select
        organization_id,
        sum(case when change_type = 'expansion' then mrr_change else 0 end) as expansion_mrr,
        sum(case when change_type = 'contraction' then abs(mrr_change) else 0 end) as contraction_mrr,
        count(distinct case when change_type = 'expansion' then subscription_id end) as expansion_count,
        count(distinct case when change_type = 'contraction' then subscription_id end) as contraction_count,
        count(distinct case when change_type = 'churn' then subscription_id end) as churn_count
    from subscription_changes
    where mrr_change is not null
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
    coalesce(ss.expansion_mrr, 0) as expansion_mrr,
    coalesce(ss.contraction_mrr, 0) as contraction_mrr,
    coalesce(ss.expansion_mrr, 0) - coalesce(ss.contraction_mrr, 0) as net_mrr_change,
    coalesce(ss.expansion_count, 0) as expansion_count,
    coalesce(ss.contraction_count, 0) as contraction_count,
    coalesce(ss.churn_count, 0) as churn_count,
    om.upgrade_count,
    om.downgrade_count,
    case
        when om.current_mrr = 0 then 0
        when om.upgrade_count > 0 then round((coalesce(ss.expansion_mrr, 0) / om.current_mrr) * 100, 2)
        else 0
    end as expansion_pct_of_mrr,
    case
        when om.max_mrr_ever = 0 then 0
        else round((om.current_mrr / om.max_mrr_ever) * 100, 2)
    end as mrr_retention_pct
from org_metrics om
left join subscription_summary ss on om.organization_id = ss.organization_id
order by om.organization_id;
"""

dim_organizations = """
CREATE OR REPLACE TABLE dim_organizations AS
with organizations as (
    select * from stg_organizations
),
org_metrics as (
    select * from int_organization_metrics
)
select
    o.organization_id,
    o.organization_name,
    o.business_os_type,
    o.industry,
    o.employee_count,
    o.status,
    o.created_at,
    o.churned_at,
    datediff('day', o.created_at, CURRENT_DATE) as days_since_signup,
    case
        when o.employee_count <= 10 then 'Small'
        when o.employee_count <= 50 then 'Medium'
        when o.employee_count <= 200 then 'Large'
        else 'Enterprise'
    end as company_size,
    case
        when datediff('day', o.created_at, CURRENT_DATE) <= 30 then 'New'
        when datediff('day', o.created_at, CURRENT_DATE) <= 90 then 'Early'
        when datediff('day', o.created_at, CURRENT_DATE) <= 365 then 'Established'
        else 'Mature'
    end as account_age_segment,
    om.current_mrr,
    om.total_events,
    om.active_users,
    om.total_users,
    om.registered_users,
    round(100.0 * om.registered_users / nullif(om.total_users, 0), 2) as user_registration_rate
from organizations o
left join org_metrics om on o.organization_id = om.organization_id;
"""

dim_subscriptions = """
CREATE OR REPLACE TABLE dim_subscriptions AS
with subscriptions as (
    select * from stg_subscriptions
),
plans as (
    select * from stg_plans
)
select
    s.subscription_id,
    s.organization_id,
    s.plan_id,
    p.plan_name,
    s.seats,
    s.mrr_amount,
    p.base_price,
    p.per_seat_price,
    s.billing_interval,
    s.started_at,
    s.ended_at,
    s.change_reason,
    s.subscription_status,
    case
        when s.billing_interval = 'annual' then round(s.mrr_amount * 12, 2)
        else s.mrr_amount
    end as normalized_annual_value,
    case
        when s.subscription_status = 'active' then datediff('day', s.started_at, CURRENT_DATE)
        else datediff('day', s.started_at, s.ended_at)
    end as subscription_days
from subscriptions s
left join plans p on s.plan_id = p.plan_id;
"""

for sql in [fct_customer_health, fct_revenue_metrics, dim_organizations, dim_subscriptions]:
    conn.execute(sql)
    print("  ✓ Created mart table")

print("\nAll models created successfully!")
conn.close()
