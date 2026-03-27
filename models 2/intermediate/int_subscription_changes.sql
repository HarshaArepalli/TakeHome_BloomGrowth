-- Track subscription changes and transitions
with subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
),
plans as (
    select * from {{ ref('stg_plans') }}
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
        else datediff('day', started_at, current_date)
    end as subscription_days
from subscription_changes
order by organization_id, started_at
